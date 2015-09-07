/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.experiments.storm.partitioner

import backtype.storm.tuple.Fields
import io.gearpump.Message
import io.gearpump.experiments.storm.util.StormTuple
import io.gearpump.partitioner.UnicastPartitioner

import scala.collection.JavaConversions._


private[storm] class FieldsGroupingPartitioner(outFields: Fields, groupFields: Fields) extends UnicastPartitioner {
  override def getPartition(msg: Message, partitionNum: Int, currentPartitionId: Int): Int = {
    val values = msg.msg.asInstanceOf[StormTuple].tuple
    val hash = outFields.select(groupFields, values).hashCode()
    (hash & Integer.MAX_VALUE) % partitionNum
  }
}