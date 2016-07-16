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

import com.datastax.driver.core.Session
import org.apache.gearpump.Message
import org.apache.gearpump.experiments.cassandra.lib.BoundStatementBuilder.BoundStatementBuilder
import org.apache.gearpump.experiments.cassandra.lib._
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.task.TaskContext

// TODO: Analyse query, compute token ranges, automatically convert types, batch, ...
class CassandraSink[T: BoundStatementBuilder] (
    connectorConf: CassandraConnectorConf,
    conf: WriteConf,
    writeCql: String)
  extends DataSink
  with Logging {

  private[this] var connector: CassandraConnector = _
  private[this] var session: Session = _

  private[this] var writer: Option[TableWriter[T]] = None

  // TODO: Non blocking
  def open(context: TaskContext): Unit = {
    connector = new CassandraConnector(connectorConf)
    session = connector.openSession()

    writer = Some(new TableWriter[T](connector, session.prepare(writeCql), conf))
  }

  def write(message: Message): Unit = writer.foreach(_.write(message.msg.asInstanceOf[T]))

  def close(): Unit = connector.evictCache()
}
