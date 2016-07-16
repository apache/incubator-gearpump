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

import akka.actor.ActorSystem
import com.twitter.bijection.Bijection
import org.apache.gearpump.experiments.cassandra.lib.BoundStatementBuilder.BoundStatementBuilder
import org.apache.gearpump.experiments.cassandra.lib.ReadConf
import org.apache.gearpump.experiments.cassandra.lib.RowExtractor.RowExtractor
import org.apache.gearpump.experiments.cassandra.lib.TimeStampExtractor._
import org.apache.gearpump.streaming.task.TaskContext
import org.mockito.Mockito._

class CassandraSourceSpec extends CassandraSpecBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    storeTestData(10, 10)
  }

  "CassandraSource" should "read data from Cassandra" in {
    val queryWithWhereCql =
      s"""
        |SELECT *
        |FROM $keyspace.$table
        |WHERE partitioning_key = ? AND clustering_key >= ?
      """.stripMargin

    case class Data(partitioningKey: String, clusteringKey: Int, data: String)

    implicit val builder: BoundStatementBuilder[Long] =
      _ => Seq("5", Bijection[Int, java.lang.Integer](5))

    implicit val rowExtractor: RowExtractor[Data] = row =>
      Data(
        row.getString("partitioning_key"),
        row.getInt("clustering_key"),
        row.getString("data"))

    implicit val timeStampExtractor: TimeStampExtractor = row =>
      row.getInt("clustering_key")

    val source = new CassandraSource[Data](
      connectorConf,
      ReadConf(),
      queryWithWhereCql)

    val actorSystem = ActorSystem("CassandraSourceEmbeddedSpec")
    val taskContext = mock[TaskContext]
    when(taskContext.system).thenReturn(actorSystem)

    source.open(taskContext, 5L)

    val result = source.read()
    assert(result.timestamp == 5L)
    assert(result.msg.asInstanceOf[Data].data == "data")
    assert(result.msg.asInstanceOf[Data].partitioningKey == "5")
    assert(result.msg.asInstanceOf[Data].clusteringKey == 5)

    val result2 = source.read()
    assert(result2.timestamp == 6L)
    assert(result2.msg.asInstanceOf[Data].data == "data")
    assert(result2.msg.asInstanceOf[Data].partitioningKey == "5")
    assert(result2.msg.asInstanceOf[Data].clusteringKey == 6)
  }
}
