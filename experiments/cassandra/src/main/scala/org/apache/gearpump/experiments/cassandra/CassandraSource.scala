/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.experiments.cassandra

import com.datastax.driver.core.{Row, Session, Statement}
import org.apache.gearpump.experiments.cassandra.lib.RowExtractor.RowExtractor
import org.apache.gearpump.experiments.cassandra.lib.TimeStampExtractor.TimeStampExtractor
import org.apache.gearpump.experiments.cassandra.lib._
import org.apache.gearpump.experiments.cassandra.lib.connector._
import org.apache.gearpump.experiments.cassandra.lib.connector.partitioner.{CassandraPartitionGenerator, CqlTokenRange}
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.streaming.transaction.api.{CheckpointStoreFactory, TimeReplayableSource, TimeStampFilter}
import org.apache.gearpump.{Message, TimeStamp}

// TODO: Analyse query, automatically convert types, ...
class CassandraSource[T: RowExtractor](
    connectorConf: CassandraConnectorConf,
    conf: ReadConf,
    keyspace: String,
    table: String,
    columns: Seq[String],
    partitionKeyColumns: Seq[String],
    clusteringKeyColumns: Seq[String],
    where: CqlWhereClause,
    timeStampFilter: TimeStampFilter,
    clusteringOrder: Option[ClusteringOrder] = None,
    limit: Option[Long] = None
  )(implicit timeStampExtractor: TimeStampExtractor)
  extends TimeReplayableSource
  with Logging {

  protected var iterator: Option[Iterator[Row]] = None
  protected var connector: CassandraConnector = _
  protected var session: Session = _

  protected val rowExtractor = implicitly[RowExtractor[T]]

  private[this] var startTime: TimeStamp = _

  private def tokenRangeToCqlQuery(range: CqlTokenRange[_, _]): (String, Seq[Any]) = {
    val (cql, values) = if (where.containsPartitionKey) {
      ("", Seq.empty)
    } else {
      range.cql(partitionKeyColumns.mkString(","))
    }
    val filter = (cql +: where.predicates).filter(_.nonEmpty).mkString(" AND ")
    val limitClause = limit.map(limit => s"LIMIT $limit").getOrElse("")
    val orderBy = clusteringOrder.map(_.toCql(clusteringKeyColumns)).getOrElse("")
    val selectColums = columns.mkString(",")
    val queryTemplate =
      s"SELECT $selectColums " +
      s"FROM $keyspace.$table " +
      s"WHERE $filter $orderBy $limitClause ALLOW FILTERING"
    val queryParamValues = values ++ where.values
    (queryTemplate, queryParamValues)
  }

  private def createStatement(session: Session, cql: String, values: Any*): Statement = {
    val stmt = session.prepare(cql)
    stmt.setConsistencyLevel(conf.consistencyLevel)
    val bstm = stmt.bind(values.map(_.asInstanceOf[AnyRef]): _*)
    bstm.setFetchSize(conf.fetchSizeInRows)
    bstm
  }

  private def fetchTokenRange(
      session: Session,
      range: CqlTokenRange[_, _]
    ): Iterator[Row] = {

    val (cql, values) = tokenRangeToCqlQuery(range)
    val stmt = createStatement(session, cql, values: _*)
    val rs = session.execute(stmt)
    new PrefetchingResultSetIterator(rs, conf.fetchSizeInRows)
  }

  // TODO: Non blocking
  override def open(context: TaskContext, startTime: Long): Unit = {
    connector = new CassandraConnector(connectorConf)
    session = connector.openSession()

    val partitioner = if (where.containsPartitionKey) {
      CassandraPartitionGenerator(connector, keyspace, table, Some(1), conf.splitSizeInMB)
    } else {
      CassandraPartitionGenerator(connector, keyspace, table, conf.splitCount, conf.splitSizeInMB)
    }

    val assignedTokenRanges =
      new DefaultPartitionGrouper()
        .group(context.parallelism, context.taskId.index, partitioner.partitions)

    this.startTime = startTime

    iterator =
      Some(
        assignedTokenRanges
          .iterator
          .flatMap(fetchTokenRange(session, _: CqlTokenRange[_, _])))
  }

  override def read(): Message =
    iterator.map{ i =>
      if (i.hasNext) {
        val message = i.next()
        val timeStamp = timeStampExtractor(message)

        timeStampFilter.filter(
          Message(rowExtractor(message), timeStamp),
          startTime).orNull
      } else {
        null
      }
    }.orNull

  override def setCheckpointStore(factory: CheckpointStoreFactory): Unit = { }

  override def close(): Unit = connector.evictCache()
}
