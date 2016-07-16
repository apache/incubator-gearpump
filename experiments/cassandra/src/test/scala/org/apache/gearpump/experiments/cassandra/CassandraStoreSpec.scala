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

import org.apache.gearpump.experiments.cassandra.lib.StoreConf
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CassandraStoreSpec
  extends FlatSpec
  with Matchers
  with BeforeAndAfterAll
  with CassandraConnection {

  private[this] val keyspace = "demo"
  private[this] val table = "CassandraStoreEmbeddedSpec"
  private[this] val name = "name"

  private[this] val checkTableExistsCql =
    s"SELECT * FROM $keyspace.$table"

  private[this] val storeConfig = StoreConf(keyspace, table)

  protected def cleanTables() = {
    val session = connector.openSession()
    session.execute(s"DROP KEYSPACE $keyspace")
  }

  override def afterAll(): Unit = {
    cleanTables()
    connector.evictCache()
  }

  "CassandraStoreFactory" should "create the appropriate tables" in {
    val factory = new AbstractCassandraStoreFactory(connectorConf, storeConfig)
    factory.getCheckpointStore(name)

    val session = connector.openSession()
    assert(session.execute(checkTableExistsCql).all().isEmpty === true)
  }

  "CassandraStore" should "persist and recover snapshots" in {
    val store =
      new AbstractCassandraStoreFactory(connectorConf, storeConfig)
        .getCheckpointStore(name)

    val checkpoint = "test"
    val bytes = checkpoint.getBytes()

    store.persist(0L, bytes)

    val recovered = store.recover(0L)
    assert(new String(recovered.get) === checkpoint)
  }

  "CassandraStore" should "not recover non existent snapshots" in {
    val store =
      new AbstractCassandraStoreFactory(connectorConf, storeConfig)
        .getCheckpointStore(name)

    val recovered = store.recover(1L)
    assert(recovered === None)
  }
}
