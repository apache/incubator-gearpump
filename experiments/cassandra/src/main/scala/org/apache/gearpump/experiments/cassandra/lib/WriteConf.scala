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
import org.apache.gearpump.experiments.cassandra.lib.WriteConf._

case class WriteConf(
    // batchSize: BatchSize = batchSizeDefault,
    // batchGroupingBufferSize: Int = batchGroupingBufferSizeDefault,
    // batchGroupingKey: BatchGroupingKey = batchGroupingKeyDefault,
    consistencyLevel: ConsistencyLevel = consistencyLevelDefault,
    // ignoreNulls: Boolean = ignoreNullsDefault,
    parallelismLevel: Int = parallelismLevelDefault)
    // throughputMiBPS: Double = throughputMiBPSDefault,
    // ttl: TTLOption = TTLOption.defaultValue,
    // timestamp: TimestampOption = TimestampOption.defaultValue
  // taskMetricsEnabled: Boolean = WriteConf.TaskMetricsParam.default) {

  // private[cassandra] val optionPlaceholders: Seq[String] = Seq(ttl, timestamp).collect {
  //   case WriteOption(PerRowWriteOptionValue(placeholder)) => placeholder
  // }

  // val throttlingEnabled = throughputMiBPS < throughputMiBPSDefault


object WriteConf {

  // val batchSizeBytesDefault = 1024
  // val batchSizeDefault = BytesInBatch(batchSizeBytesDefault)
  // val batchGroupingBufferSizeDefault = 1000
  // val batchGroupingKeyDefault = Partition
  private val consistencyLevelDefault = ConsistencyLevel.LOCAL_QUORUM
  // val ignoreNullsDefault = false
  private val parallelismLevelDefault = 5
  // val throughputMiBPSDefault = Int.MaxValue

}
