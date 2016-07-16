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

import org.apache.gearpump.Message
import org.apache.gearpump.experiments.cassandra.lib.BoundStatementBuilder.BoundStatementBuilder
import org.apache.gearpump.experiments.cassandra.lib.RowExtractor.RowExtractor
import org.apache.gearpump.experiments.cassandra.lib.TimeStampExtractor._
import org.apache.gearpump.experiments.cassandra.lib._
import org.apache.gearpump.experiments.cassandra.lib.partitioner.CassandraPartitionGenerator
import org.apache.gearpump.streaming.task.TaskContext

// TODO: Analyse query, compute token ranges, automatically convert types, ...
class CassandraSource[T: RowExtractor] (
    connectorConf: CassandraConnectorConf,
    conf: ReadConf,
    queryCql: String
  )(implicit boundStatementBuilder: BoundStatementBuilder[Long],
    timeStampExtractor: TimeStampExtractor)
  extends CassandraSourceBase[T](connectorConf, conf) {

  // TODO: Non blocking
  def open(context: TaskContext, startTime: Long): Unit = {
    connector = new CassandraConnector(connectorConf)
    session = connector.openSession()

    val resultSet =
      session.execute(
        session.prepare(queryCql)
          .bind(boundStatementBuilder(startTime): _*)
          .setConsistencyLevel(conf.consistencyLevel))

    iterator = Some(new PrefetchingResultSetIterator(resultSet, conf.fetchSizeInRows))
  }

  def read(): Message =
    iterator.map { i =>
      if (i.hasNext) {
        val message = i.next()
        Message(rowExtractor(message), timeStampExtractor(message))
      } else {
        null
      }
    }.orNull
}
