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

import com.datastax.driver.core.ConsistencyLevel
import org.apache.gearpump.experiments.cassandra.lib.StoreConf._

case class StoreConf(
    keyspaceName: String = keyspaceNameDefault,
    tableName: String = tableNameDefault,
    replicationStrategyCql: String = replicationStrategyCql,
    persistConsistencyLevel: ConsistencyLevel = persistConsistencyLevelDefault,
    recoverConsistencyLevel: ConsistencyLevel = recoverConsistencyLevelDefault,
    compactionStrategyCql: String = compactionStrategyCqlDefault)

object StoreConf {
  private val keyspaceNameDefault = "gearpump"
  private val tableNameDefault = "checkpoints"
  private val replicationStrategyCql = "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
  private val persistConsistencyLevelDefault = ConsistencyLevel.LOCAL_ONE
  private val recoverConsistencyLevelDefault = ConsistencyLevel.LOCAL_ONE
  private val compactionStrategyCqlDefault = "{'class' : 'SizeTieredCompactionStrategy' }"
}
