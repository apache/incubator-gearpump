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

import scala.collection.JavaConverters._

import com.twitter.bijection.Bijection
import org.apache.gearpump.Message
import org.apache.gearpump.experiments.cassandra.lib.BoundStatementBuilder.BoundStatementBuilder
import org.apache.gearpump.experiments.cassandra.lib.WriteConf
import org.apache.gearpump.streaming.task.TaskContext

class CassandraSinkSpec extends CassandraSpecBase {

  private[this] val insertCql =
    s"""
      |INSERT INTO $keyspace.$table(partitioning_key, clustering_key, data)
      |VALUES(?, ?, ?)
      """.stripMargin

  private def selectAll() = {
    val session = connector.openSession()
    session.execute(selectAllCql)
  }

  "CassandraSink" should "write data to Cassandra" in {
    implicit val builder: BoundStatementBuilder[(String, Int, String)] =
      value => Seq(value._1, Bijection[Int, java.lang.Integer](value._2), value._3)

    val sink = new CassandraSink[(String, Int, String)](
      connectorConf,
      WriteConf(),
      insertCql)

    val taskContext = mock[TaskContext]
    sink.open(taskContext)

    val message = Message(("1", 1, "data"))
    sink.write(message)

    val data = selectAll().all().asScala
    assert(data.size == 1)
    val first = data.head
    assert(first.getString("partitioning_key") == "1")
    assert(first.getInt("clustering_key") == 1)
    assert(first.getString("data") == "data")

    val message2 = Message(("1", 2, "data"))
    sink.write(message2)

    val data2 = selectAll().all().asScala
    assert(data2.size == 2)
    val last = data2.last
    assert(last.getString("partitioning_key") == "1")
    assert(last.getInt("clustering_key") == 2)
    assert(last.getString("data") == "data")
  }
}
