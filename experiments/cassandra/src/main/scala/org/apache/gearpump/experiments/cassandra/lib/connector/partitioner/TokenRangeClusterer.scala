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

import scala.Ordering.Implicits._
import scala.annotation.tailrec

import org.apache.gearpump.experiments.cassandra.lib.connector.partitioner.dht.{Token, TokenRange}

class TokenRangeClusterer[V, T <: Token[V]](
    maxRowCountPerGroup: Long,
    maxGroupSize: Int = Int.MaxValue) {

  private implicit object InetAddressOrdering extends Ordering[InetAddress] {
    override def compare(
        x: InetAddress,
        y: InetAddress): Int =
        x.getHostAddress.compareTo(y.getHostAddress)
  }

  @tailrec
  private def group(
      tokenRanges: Stream[TokenRange[V, T]],
      result: Vector[Seq[TokenRange[V, T]]]
    ): Iterable[Seq[TokenRange[V, T]]] = {

    tokenRanges match {
      case Stream.Empty => result
      case head #:: rest =>
        val firstEndpoint = head.replicas.min
        val rowCounts = tokenRanges.map(_.dataSize)
        val cumulativeRowCounts = rowCounts.scanLeft(0L)(_ + _).tail
        val rowLimit = math.max(maxRowCountPerGroup, head.dataSize)
        val cluster = tokenRanges
          .take(math.max(1, maxGroupSize))
          .zip(cumulativeRowCounts)
          .takeWhile { case (tr, count) => count <= rowLimit && tr.replicas.min == firstEndpoint }
          .map(_._1)
          .toVector
        val remainingTokenRanges = tokenRanges.drop(cluster.length)
        group(remainingTokenRanges, result :+ cluster)
    }
  }

  def group(tokenRanges: Seq[TokenRange[V, T]]): Iterable[Seq[TokenRange[V, T]]] = {
    val sortedRanges = tokenRanges.sortBy(_.replicas.toSeq.sorted)
    group(sortedRanges.toStream, Vector.empty)
  }
}
