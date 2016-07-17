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
package org.apache.gearpump.experiments.cassandra.lib.partitioner.dht

import scala.language.existentials

import org.apache.gearpump.experiments.cassandra.lib.partitioner.MonotonicBucketing

trait TokenFactory[V, T <: Token[V]] extends Serializable {
  def minToken: T
  def maxToken: T

  def totalTokenCount: BigInt

  def distance(token1: T, token2: T): BigInt

  def ringFraction(token1: T, token2: T): Double =
    distance(token1, token2).toDouble / totalTokenCount.toDouble

  def tokenFromString(string: String): T

  def tokenToString(token: T): String

  implicit def tokenOrdering: Ordering[T]

  implicit def tokenBucketing: MonotonicBucketing[T]
}

object TokenFactory {

  // scalastyle:off
  type V = t forSome { type t }
  type T = t forSome { type t <: Token[V] }
  // scalastyle:on

  implicit object Murmur3TokenFactory extends TokenFactory[Long, LongToken] {
    override val minToken = LongToken(Long.MinValue)
    override val maxToken = LongToken(Long.MaxValue)
    override val totalTokenCount = BigInt(maxToken.value) - BigInt(minToken.value)
    override def tokenFromString(string: String): LongToken = LongToken(string.toLong)
    override def tokenToString(token: LongToken): String = token.value.toString

    override def distance(token1: LongToken, token2: LongToken): BigInt = {
      val left = token1.value
      val right = token2.value
      if (right > left) BigInt(right) - BigInt(left)
      else BigInt(right) - BigInt(left) + totalTokenCount
    }

    override def tokenBucketing: MonotonicBucketing[LongToken] =
      implicitly[MonotonicBucketing[LongToken]]

    override def tokenOrdering: Ordering[LongToken] =
      Ordering.by(_.value)
  }

  implicit object RandomPartitionerTokenFactory extends TokenFactory[BigInt, BigIntToken] {
    override val minToken = BigIntToken(-1)
    override val maxToken = BigIntToken(BigInt(2).pow(127))
    override val totalTokenCount = maxToken.value - minToken.value
    override def tokenFromString(string: String): BigIntToken = BigIntToken(BigInt(string))
    override def tokenToString(token: BigIntToken): String = token.value.toString()

    override def distance(token1: BigIntToken, token2: BigIntToken): BigInt = {
      val left = token1.value
      val right = token2.value

      if (right > left) {
        right - left
      } else {
        right - left + totalTokenCount
      }
    }

    override def tokenBucketing: MonotonicBucketing[BigIntToken] =
      implicitly[MonotonicBucketing[BigIntToken]]

    override def tokenOrdering: Ordering[BigIntToken] =
      Ordering.by(_.value)
  }

  def forCassandraPartitioner(partitionerClassName: String): TokenFactory[V, T] = {
    val partitioner =
      partitionerClassName match {
        case "org.apache.cassandra.dht.Murmur3Partitioner" => Murmur3TokenFactory
        case "org.apache.cassandra.dht.RandomPartitioner" => RandomPartitionerTokenFactory
        case _ =>
          throw new IllegalArgumentException(s"Unsupported partitioner: $partitionerClassName")
      }
    partitioner.asInstanceOf[TokenFactory[V, T]]
  }
}




