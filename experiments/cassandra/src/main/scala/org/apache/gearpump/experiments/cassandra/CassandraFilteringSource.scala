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

import org.apache.gearpump.experiments.cassandra.lib.RowExtractor._
import org.apache.gearpump.experiments.cassandra.lib.TimeStampExtractor.TimeStampExtractor
import org.apache.gearpump.experiments.cassandra.lib.partitioner.CassandraPartitionGenerator
import org.apache.gearpump.experiments.cassandra.lib.{CassandraConnector, PrefetchingResultSetIterator, ReadConf}
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.streaming.transaction.api.TimeStampFilter
import org.apache.gearpump.{Message, TimeStamp}

// TODO: Analyse query, compute token ranges, automatically convert types, ...
class CassandraFilteringSource[T: RowExtractor](
    connector: CassandraConnector,
    conf: ReadConf,
    queryCql: String,
    table: String,
    keyspace: String,
    timeStampFilter: TimeStampFilter
  )(implicit timeStampExtractor: TimeStampExtractor)
  extends CassandraSourceBase[T](connector, conf) {

  val partitioner = CassandraPartitionGenerator(connector, keyspace, table, Some(3), 3)

  val partitions = partitioner.partitions
  println(partitions.size)
  println(partitions)

  private[this] var startTime: TimeStamp = _

  // TODO: Non blocking
  def open(context: TaskContext, startTime: Long): Unit = {
    this.startTime = startTime

    val resultSet =
      session.execute(
        session.prepare(queryCql)
          .bind()
          .setConsistencyLevel(conf.consistencyLevel))

    iterator = Some(new PrefetchingResultSetIterator(resultSet, conf.fetchSizeInRows))
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
}
