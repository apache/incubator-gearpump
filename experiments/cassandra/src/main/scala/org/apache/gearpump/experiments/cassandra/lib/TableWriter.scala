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
package org.apache.gearpump.experiments.cassandra.lib

import java.io.IOException

import com.datastax.driver.core.PreparedStatement
import org.apache.gearpump.experiments.cassandra.lib.BoundStatementBuilder.BoundStatementBuilder

class TableWriter[T: BoundStatementBuilder] (
    connector: CassandraConnector,
    statement: PreparedStatement,
    writeConf: WriteConf) {

  def write(data: T) {
    val session = connector.openSession()
    val queryExecutor = new QueryExecutor(session, writeConf.parallelismLevel, None, None)

    // TODO: Batch writes?
    val stmtToWrite = statement
      .setConsistencyLevel(writeConf.consistencyLevel)
      .bind(implicitly[BoundStatementBuilder[T]].apply(data): _*)

    queryExecutor.executeAsync(stmtToWrite)
    queryExecutor.waitForCurrentlyExecutingTasks()

    if (!queryExecutor.successful) {
      throw new IOException(s"Failed to write statements to $statement.")
    }
  }
}
