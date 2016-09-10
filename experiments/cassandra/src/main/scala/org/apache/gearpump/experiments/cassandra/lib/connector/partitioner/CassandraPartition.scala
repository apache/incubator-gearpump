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
package org.apache.gearpump.experiments.cassandra.lib.connector.partitioner

import java.net.InetAddress

import org.apache.gearpump.experiments.cassandra.lib.connector.partitioner.dht.{TokenRange, TokenFactory, Token}

/**
 * The original file (spark-cassandra-connector 1.6.0) was modified
 */
case class CqlTokenRange[V, T <: Token[V]](
    range: TokenRange[V, T]
  )(implicit tf: TokenFactory[V, T]) {

  require(!range.isWrappedAround)

  def cql(pk: String): (String, Seq[Any]) =
    if (range.start == tf.minToken && range.end == tf.minToken) {
      (s"token($pk) >= ?", Seq(range.start.value))
    } else if (range.start == tf.minToken) {
      (s"token($pk) <= ?", Seq(range.end.value))
    } else if (range.end == tf.minToken) {
      (s"token($pk) > ?", Seq(range.start.value))
    } else {
      (s"token($pk) > ? AND token($pk) <= ?", Seq(range.start.value, range.end.value))
    }
}

trait EndpointPartition {
  def endpoints: Iterable[InetAddress]
}

case class CassandraPartition[V, T <: Token[V]](
    index: Int,
    endpoints: Iterable[InetAddress],
    tokenRanges: Iterable[CqlTokenRange[V, T]],
    dataSize: Long)
  extends EndpointPartition

