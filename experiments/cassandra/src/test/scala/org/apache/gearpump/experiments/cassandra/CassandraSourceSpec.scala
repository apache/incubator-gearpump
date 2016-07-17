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

import org.apache.gearpump._
import org.apache.gearpump.experiments.cassandra.lib.{CqlWhereClause, ReadConf}
import org.apache.gearpump.experiments.cassandra.lib.RowExtractor._
import org.apache.gearpump.experiments.cassandra.lib.TimeStampExtractor.TimeStampExtractor
import org.apache.gearpump.streaming.source.DefaultTimeStampFilter
import org.apache.gearpump.streaming.task.{TaskId, TaskContext}
import org.apache.gearpump.streaming.transaction.api.TimeStampFilter
import org.mockito.Mockito._

class CassandraSourceSpec extends CassandraSpecBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    storeTestData(10, 10)
  }

  case class Data(partitioningKey: String, clusteringKey: Int, data: String)

  implicit val rowExtractor: RowExtractor[Data] = row =>
    Data(
      row.getString("partitioning_key"),
      row.getInt("clustering_key"),
      row.getString("data"))

  implicit val timeStampExtractor: TimeStampExtractor = row =>
    row.getInt("clustering_key")

  "CassandraSource" should "read data from Cassandra without a where predicate" in {

    val source = new CassandraSource[Data](
      connectorConf,
      ReadConf(),
      keyspace,
      table,
      Seq("partitioning_key", "clustering_key", "data"),
      Seq("partitioning_key"),
      Seq("clustering_key"),
      CqlWhereClause.empty,
      new DefaultTimeStampFilter(),
      None,
      None)

    val taskContext = mock[TaskContext]
    when(taskContext.parallelism).thenReturn(1)
    when(taskContext.taskId).thenReturn(TaskId(1, 0))

    source.open(taskContext, 5)
    assert((0 to 4).map(_ => source.read()) == (0 to 4).map(_ => null))

    val result = source.read()
    assert(result.timestamp == 5L)
    assert(result.msg.asInstanceOf[Data].clusteringKey == 5)
    assert(result.msg.asInstanceOf[Data].data == "data")

    val result2 = source.read()
    assert(result2.timestamp == 6L)
    assert(result2.msg.asInstanceOf[Data].data == "data")
    assert(result2.msg.asInstanceOf[Data].clusteringKey == 6)
  }

  it should "read data from Cassandra when where clause is specified" in {
    val source = new CassandraSource[Data](
      connectorConf,
      ReadConf(),
      keyspace,
      table,
      Seq("partitioning_key", "clustering_key", "data"),
      Seq("partitioning_key"),
      Seq("clustering_key"),
      CqlWhereClause(
        Seq("partitioning_key = ?", "clustering_key >= ?"),
        Seq("5", 5),
        containsPartitionKey = true),
      new TimeStampFilter() {
        override def filter(msg: Message, predicate: TimeStamp): Option[Message] =
          Some(msg)
      },
      None,
      None)

    val taskContext = mock[TaskContext]
    when(taskContext.parallelism).thenReturn(1)
    when(taskContext.taskId).thenReturn(TaskId(1, 0))

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
