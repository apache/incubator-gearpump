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
package org.apache.gearpump.experiments.cassandra.dsl

import scala.concurrent.ExecutionContext

import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.experiments.cassandra.CassandraSink
import org.apache.gearpump.experiments.cassandra.lib.BoundStatementBuilder.BoundStatementBuilder
import org.apache.gearpump.experiments.cassandra.lib.{CassandraConnectorConf, CassandraConnector,
WriteConf}
import org.apache.gearpump.streaming.dsl

class CassandraDSLSink[T: BoundStatementBuilder](stream: dsl.Stream[T]) {

  def writeToCassandra(
      connectorConf: CassandraConnectorConf,
      conf: WriteConf,
      query: String,
      parallelism: Int,
      description: String)(implicit ec: ExecutionContext): dsl.Stream[T] =
    stream.sink(
      new CassandraSink[T](connectorConf, conf, query), parallelism, UserConfig.empty, description)
}
