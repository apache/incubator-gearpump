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
package org.apache.gearpump.experiments.cassandra.lib.partitioner

import org.apache.gearpump.experiments.cassandra.lib.partitioner.dht.{BigIntToken, TokenFactory, TokenRange}

class RandomPartitionerTokenRangeSplitter(dataSize: Long)
  extends TokenRangeSplitter[BigInt, BigIntToken] {

  private val tokenFactory =
    TokenFactory.RandomPartitionerTokenFactory

  private def wrap(token: BigInt): BigInt = {
    val max = tokenFactory.maxToken.value
    if (token <= max) token else token - max
  }

  private type TR = TokenRange[BigInt, BigIntToken]

  def split(range: TR, splitSize: Long): Seq[TR] = {
    val rangeSize = range.dataSize
    val rangeTokenCount = tokenFactory.distance(range.start, range.end)
    val n = math.max(1, math.round(rangeSize.toDouble / splitSize)).toInt

    val left = range.start.value
    val right = range.end.value
    val splitPoints =
      (for (i <- 0 until n) yield wrap(left + (rangeTokenCount * i / n))) :+ right

    for (Seq(l, r) <- splitPoints.sliding(2).toSeq) yield
      new TokenRange[BigInt, BigIntToken](
        new BigIntToken(l.bigInteger),
        new BigIntToken(r.bigInteger),
        range.replicas,
        rangeSize / n)
  }
}