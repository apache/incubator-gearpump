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
 *
 * The original file (spark-cassandra-connector 1.6.0) was modified
 */
package org.apache.gearpump.experiments.cassandra.lib.connector.partitioner

import scala.collection.JavaConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.language.existentials

import com.datastax.driver.core.{Metadata, TokenRange => DriverTokenRange}
import org.apache.gearpump.experiments.cassandra.lib.connector.partitioner.dht.{Token, TokenFactory}
import org.apache.gearpump.experiments.cassandra.lib.{CassandraConnector, Logging}

private[cassandra] class CassandraPartitionGenerator[V, T <: Token[V]] private (
    connector: CassandraConnector,
    keyspaceName: String,
    tableName: String,
    splitCount: Option[Int],
    splitSize: Long
  )(implicit tokenFactory: TokenFactory[V, T]) extends Logging {

  type Token = dht.Token[T]
  type TokenRange = dht.TokenRange[V, T]

  private val totalDataSize: Long = {
    splitCount match {
      case Some(c) => c * splitSize
      case None => new DataSizeEstimates(connector, keyspaceName, tableName).dataSizeInBytes
    }
  }

  private def tokenRange(range: DriverTokenRange, metadata: Metadata): TokenRange = {
    val startToken = tokenFactory.tokenFromString(range.getStart.getValue.toString)
    val endToken = tokenFactory.tokenFromString(range.getEnd.getValue.toString)
    val replicas =
      metadata.getReplicas(Metadata.quote(keyspaceName), range).asScala.map(_.getAddress).toSet
    val dataSize = (tokenFactory.ringFraction(startToken, endToken) * totalDataSize).toLong
    new TokenRange(startToken, endToken, replicas, dataSize)
  }

  private def describeRing: Seq[TokenRange] = {
    val session = connector.openSession()
    val cluster = session.getCluster

    val metadata = cluster.getMetadata
    val ranges = for (tr <- metadata.getTokenRanges.asScala.toSeq) yield tokenRange(tr, metadata)

    if (splitCount.contains(1)) {
      Seq(ranges.head.copy[V, T](tokenFactory.minToken, tokenFactory.maxToken))
    } else {
      ranges
    }
  }

  private def splitsOf(
      tokenRanges: Iterable[TokenRange],
      splitter: TokenRangeSplitter[V, T]
    ): Iterable[TokenRange] = {

    val parTokenRanges = tokenRanges.par
    parTokenRanges.tasksupport = new ForkJoinTaskSupport(CassandraPartitionGenerator.pool)

    (for {
      tokenRange <- parTokenRanges
      split <- splitter.split(tokenRange, splitSize)
    } yield split).seq
  }

  private def createTokenRangeSplitter: TokenRangeSplitter[V, T] = {
    tokenFactory.asInstanceOf[TokenFactory[_, _]] match {
      case TokenFactory.RandomPartitionerTokenFactory =>
        new RandomPartitionerTokenRangeSplitter(totalDataSize)
          .asInstanceOf[TokenRangeSplitter[V, T]]
      case TokenFactory.Murmur3TokenFactory =>
        new Murmur3PartitionerTokenRangeSplitter(totalDataSize)
          .asInstanceOf[TokenRangeSplitter[V, T]]
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported TokenFactory $tokenFactory")
    }
  }

  private def rangeToCql(range: TokenRange): Seq[CqlTokenRange[V, T]] =
    range.unwrap.map(CqlTokenRange(_))

  def partitions: Seq[CassandraPartition[V, T]] = {
    val tokenRanges = describeRing
    val endpointCount = tokenRanges.map(_.replicas).reduce(_ ++ _).size
    val splitter = createTokenRangeSplitter
    val splits = splitsOf(tokenRanges, splitter).toSeq
    val maxGroupSize = tokenRanges.size / endpointCount
    val clusterer = new TokenRangeClusterer[V, T](splitSize, maxGroupSize)
    val tokenRangeGroups = clusterer.group(splits).toArray
    val partitions = for (group <- tokenRangeGroups) yield {
      val replicas = group.map(_.replicas).reduce(_ intersect _)
      val rowCount = group.map(_.dataSize).sum
      val cqlRanges = group.flatMap(rangeToCql)
      CassandraPartition(0, replicas, cqlRanges, rowCount)
    }

    partitions
      .sortBy(p => (p.endpoints.size, -p.dataSize))
      .zipWithIndex
      .map { case (p, index) => p.copy(index = index) }
  }
}

object CassandraPartitionGenerator {

  val MaxParallelism = 16
  val TokenRangeSampleSize = 16

  private val pool: ForkJoinPool = new ForkJoinPool(MaxParallelism)

  // scalastyle:off
  type V = t forSome { type t }
  type T = t forSome { type t <: Token[V] }
  // scalastyle:on

  def apply(
      conn: CassandraConnector,
      keyspaceName: String,
      tableName: String,
      splitCount: Option[Int],
      splitSize: Int
    ): CassandraPartitionGenerator[V, T] = {

    val tokenFactory = getTokenFactory(conn)
    new CassandraPartitionGenerator(
      conn,
      keyspaceName,
      tableName,
      splitCount,
      splitSize)(tokenFactory)
  }

  def getTokenFactory(conn: CassandraConnector) : TokenFactory[V, T] = {
    val session = conn.openSession()
    val partitionerName =
      session.execute("SELECT partitioner FROM system.local").one().getString(0)

    TokenFactory.forCassandraPartitioner(partitionerName)
  }
}
