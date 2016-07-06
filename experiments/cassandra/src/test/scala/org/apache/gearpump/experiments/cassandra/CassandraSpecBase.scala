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

import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

trait CassandraSpecBase
  extends FlatSpec
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar
  with CassandraConnection {

  protected val keyspace = "demo"
  protected val table = "CassandraSourceEmbeddedSpec"

  protected def createTables() = {
    val session = connector.openSession()

    session.execute(
      s"""
      |CREATE KEYSPACE IF NOT EXISTS $keyspace
      |WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
      """.stripMargin)

    session.execute(
      s"""
        |CREATE TABLE IF NOT EXISTS $keyspace.$table(
        |  partitioning_key text,
        |  clustering_key int,
        |  data text,
        |  PRIMARY KEY(partitioning_key, clustering_key)
        |)
      """.stripMargin)
  }

  protected def cleanTables() = {
    val session = connector.openSession()
    session.execute(s"DROP KEYSPACE $keyspace")
  }

  protected val selectAllCql = s"SELECT * FROM $keyspace.$table"

  override def beforeAll(): Unit = {
    createTables()
  }

  override def afterAll(): Unit = {
    cleanTables()
    connector.evictCache()
  }

  protected def storeTestData(partitions: Int, rows: Int) = {
    val session = connector.openSession()

    (0 to partitions).map { partition =>
      (0 to rows).map { row =>
        session.execute(
          s"""
            |INSERT INTO $keyspace.$table(partitioning_key, clustering_key, data)
            |VALUES('$partition', $row, 'data')
          """.stripMargin)
      }
    }
  }
}
