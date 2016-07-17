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

import java.nio.ByteBuffer

import scala.collection.JavaConverters._

import com.twitter.bijection.Bijection
import org.apache.gearpump.TimeStamp
import org.apache.gearpump.experiments.cassandra.AbstractCassandraStoreFactory._
import org.apache.gearpump.experiments.cassandra.CassandraStore._
import org.apache.gearpump.experiments.cassandra.lib.connector.CassandraConnectorConf
import org.apache.gearpump.experiments.cassandra.lib.{CassandraConnector, Logging, StoreConf}
import org.apache.gearpump.streaming.transaction.api.{CheckpointStore, CheckpointStoreFactory}

object AbstractCassandraStoreFactory {
  private def createKeyspace(keyspaceName: String, replicationCql: String): String =
    s"""
      |CREATE KEYSPACE IF NOT EXISTS $keyspaceName
      |WITH REPLICATION = $replicationCql
    """.stripMargin

  private def createTable(keyspaceName: String, tableName: String, compactionCql: String): String =
    s"""
      |CREATE TABLE IF NOT EXISTS $keyspaceName.$tableName(
      |  name text,
      |  timestamp bigint,
      |  checkpoint blob,
      |  PRIMARY KEY (name, timestamp))
      |WITH compaction = $compactionCql
    """.stripMargin
}

class AbstractCassandraStoreFactory(
    connectorConf: CassandraConnectorConf,
    storeConf: StoreConf)
  extends CheckpointStoreFactory {

  // TODO: Non blocking
  override def getCheckpointStore(name: String): CheckpointStore = {
    val connector = new CassandraConnector(connectorConf)
    val session = connector.openSession()
    session.execute(createKeyspace(storeConf.keyspaceName, storeConf.replicationStrategyCql))
    session.execute(
      createTable(storeConf.keyspaceName, storeConf.tableName, storeConf.compactionStrategyCql))

    connector.evictCache()
    new CassandraStore(name, connectorConf, storeConf)
  }
}

object CassandraStore {
  private def writeCheckpoint(keyspaceName: String, tableName: String): String =
    s"""
      |INSERT INTO $keyspaceName.$tableName (name, timestamp, checkpoint)
      |VALUES (?, ?, ?)
    """.stripMargin

  private def readCheckpoint(keyspaceName: String, tableName: String): String =
    s"""
      |SELECT * FROM $keyspaceName.$tableName
      |WHERE name = ?
      |AND timestamp = ?
    """.stripMargin
}

class CassandraStore private[cassandra] (
    name: String,
    connectorConf: CassandraConnectorConf,
    storeConf: StoreConf)
  extends CheckpointStore
  with Logging {

  private[this] val connector = new CassandraConnector(connectorConf)
  private[this] val session = connector.openSession()

  private[this] val preparedRead =
    session.prepare(readCheckpoint(storeConf.keyspaceName, storeConf.tableName))
  private[this] val preparedWrite =
    session.prepare(writeCheckpoint(storeConf.keyspaceName, storeConf.tableName))

  // TODO: Non blocking
  override def persist(timeStamp: TimeStamp, checkpoint: Array[Byte]): Unit =
    session.execute(
      preparedWrite
        .bind(name, Bijection[Long, java.lang.Long](timeStamp), ByteBuffer.wrap(checkpoint))
        .setConsistencyLevel(storeConf.persistConsistencyLevel))

  // TODO: Non blocking
  override def recover(timestamp: TimeStamp): Option[Array[Byte]] = {
    session
      .execute(
        preparedRead
          .bind(name, Bijection[Long, java.lang.Long](timestamp))
          .setConsistencyLevel(storeConf.recoverConsistencyLevel))
      .all()
      .asScala
      .headOption.map(_.getBytes("checkpoint").array())
  }

  override def close(): Unit = connector.evictCache()
}
