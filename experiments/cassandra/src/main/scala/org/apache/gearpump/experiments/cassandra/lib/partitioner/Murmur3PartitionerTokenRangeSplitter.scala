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

import org.apache.gearpump.experiments.cassandra.lib.partitioner.dht.{LongToken, TokenFactory, TokenRange}

class Murmur3PartitionerTokenRangeSplitter(dataSize: Long)
  extends TokenRangeSplitter[Long, LongToken] {

  private val tokenFactory =
    TokenFactory.Murmur3TokenFactory

  private type TR = TokenRange[Long, LongToken]

  def split(range: TR, splitSize: Long): Seq[TR] = {
    val rangeSize = range.dataSize
    val rangeTokenCount = tokenFactory.distance(range.start, range.end)
    val n = math.max(1, math.round(rangeSize.toDouble / splitSize).toInt)

    val left = range.start.value
    val right = range.end.value
    val splitPoints =
      (for (i <- 0 until n) yield left + (rangeTokenCount * i / n).toLong) :+ right

    for (Seq(l, r) <- splitPoints.sliding(2).toSeq) yield
      new TokenRange[Long, LongToken](
        new LongToken(l),
        new LongToken(r),
        range.replicas,
        rangeSize / n)
  }
}
