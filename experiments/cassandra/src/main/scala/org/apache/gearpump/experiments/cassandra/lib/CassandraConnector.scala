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

import com.datastax.driver.core.Session

// TODO: Proper cluster, session and statement management
class CassandraConnector(conf: CassandraConnectorConf) extends Serializable {

  private[this] var session: Session = _
  private[this] var counter = 0

  private def openSessionInternal() = synchronized {
    if (counter == 0) {
      val cluster = conf.connectionFactory.createCluster(conf)
      session = cluster.connect()
    }

    counter = counter + 1
    session
  }

  def openSession(): Session =
    openSessionInternal()

  def evictCache(): Unit = synchronized {
    if (counter > 0) {
      counter = 0
      val cluster = session.getCluster
      session.close()
      cluster.close()
    }
  }

  def close(session: Session): Unit = synchronized {
    if (counter > 0) {
      counter = counter - 1

      if (counter == 0) {
        val cluster = session.getCluster
        session.close()
        cluster.close()
      }
    }
  }

  // TODO: Ensure the caller can not close the session
  def withSession[T](block: Session => T): T = {
    val session = openSession()
    val result = block(session)
    close(session)
    result
  }
}
