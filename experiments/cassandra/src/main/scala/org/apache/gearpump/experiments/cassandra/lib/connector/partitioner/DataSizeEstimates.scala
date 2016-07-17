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

import scala.collection.JavaConverters._

import com.datastax.driver.core.exceptions.InvalidQueryException
import org.apache.gearpump.experiments.cassandra.lib.{Logging, CassandraConnector}
import org.apache.gearpump.experiments.cassandra.lib.connector.partitioner.dht.{TokenFactory, Token}

class DataSizeEstimates[V, T <: Token[V]](
    conn: CassandraConnector,
    keyspaceName: String,
    tableName: String)(
    implicit tokenFactory: TokenFactory[V, T])
  extends Logging {

  private case class TokenRangeSizeEstimate(
      rangeStart: T,
      rangeEnd: T,
      partitionsCount: Long,
      meanPartitionSize: Long) {

    def ringFraction: Double =
      tokenFactory.ringFraction(rangeStart, rangeEnd)

    def totalSizeInBytes: Long =
      partitionsCount * meanPartitionSize
  }

  private lazy val tokenRanges: Seq[TokenRangeSizeEstimate] = {
    val session = conn.openSession()

    try {
      val rs = session.execute(
        "SELECT range_start, range_end, partitions_count, mean_partition_size " +
          "FROM system.size_estimates " +
          "WHERE keyspace_name = ? AND table_name = ?", keyspaceName, tableName)

      for (row <- rs.all().asScala) yield TokenRangeSizeEstimate(
        rangeStart = tokenFactory.tokenFromString(row.getString("range_start")),
        rangeEnd = tokenFactory.tokenFromString(row.getString("range_end")),
        partitionsCount = row.getLong("partitions_count"),
        meanPartitionSize = row.getLong("mean_partition_size")
      )
    }
    catch {
      case e: InvalidQueryException =>
        LOG.error(
          s"Failed to fetch size estimates for $keyspaceName.$tableName from" +
            s"system.size_estimates table. The number of created Spark partitions" +
            s"may be inaccurate. Please make sure you use Cassandra 2.1.5 or newer.", e)
        Seq.empty
    }
  }

  private lazy val ringFraction =
    tokenRanges.map(_.ringFraction).sum

  lazy val partitionCount: Long = {
    val partitionsCount = tokenRanges.map(_.partitionsCount).sum
    val normalizedCount = (partitionsCount / ringFraction).toLong
    LOG.debug(s"Estimated partition count of $keyspaceName.$tableName is $normalizedCount")
    normalizedCount
  }

  lazy val dataSizeInBytes: Long = {
    val tokenRangeSizeInBytes = (totalDataSizeInBytes / ringFraction).toLong
    LOG.debug(s"Estimated size of $keyspaceName.$tableName is $tokenRangeSizeInBytes bytes")
    tokenRangeSizeInBytes
  }

  lazy val totalDataSizeInBytes: Long = {
    tokenRanges.map(_.totalSizeInBytes).sum
  }
}
