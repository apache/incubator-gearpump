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
package org.apache.gearpump.experiments.cassandra.lib.connector

import java.net.InetAddress

import scala.concurrent.duration.{Duration, _}

import com.datastax.driver.core.ProtocolOptions
import org.apache.gearpump.experiments.cassandra.lib.connector.CassandraConnectorConf.{CassandraSSLConf, RetryDelayConf}

/**
 * The original file (spark-cassandra-connector 1.6.0) was modified
 */
case class CassandraConnectorConf(
    hosts: Set[InetAddress] = Set(InetAddress.getLocalHost),
    port: Int = 9042,
    authConf: AuthConf = NoAuthConf,
    minReconnectionDelayMillis: Int = 1000,
    maxReconnectionDelayMillis: Int = 6000,
    compression: ProtocolOptions.Compression = ProtocolOptions.Compression.NONE,
    queryRetryCount: Int = 10,
    connectTimeoutMillis: Int = 5000,
    readTimeoutMillis: Int = 120000,
    connectionFactory: CassandraConnectionFactory = DefaultConnectionFactory,
    cassandraSSLConf: CassandraConnectorConf.CassandraSSLConf = CassandraSSLConf(),
    queryRetryDelay: CassandraConnectorConf.RetryDelayConf =
      RetryDelayConf.ExponentialDelay(4.seconds, 1.5d))

object CassandraConnectorConf {

  case class CassandraSSLConf(
      enabled: Boolean = false,
      trustStorePath: Option[String] = None,
      trustStorePassword: Option[String] = None,
      trustStoreType: String = "JKS",
      protocol: String = "TLS",
      enabledAlgorithms: Set[String] =
      Set("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA"))

  trait RetryDelayConf {
    def forRetry(retryNumber: Int): Duration
  }

  object RetryDelayConf extends Serializable {

    case class ConstantDelay(delay: Duration) extends RetryDelayConf {
      require(delay.length >= 0, "Delay must not be negative")

      override def forRetry(nbRetry: Int): Duration = delay
      override def toString: String = s"${delay.length}"
    }

    case class LinearDelay(initialDelay: Duration, increaseBy: Duration) extends RetryDelayConf {
      require(initialDelay.length >= 0, "Initial delay must not be negative")
      require(increaseBy.length > 0, "Delay increase must be greater than 0")

      override def forRetry(nbRetry: Int): Duration =
        initialDelay + (increaseBy * (nbRetry - 1).max(0))
      override def toString: String = s"${initialDelay.length} + $increaseBy"
    }

    case class ExponentialDelay(initialDelay: Duration, increaseBy: Double) extends RetryDelayConf {
      require(initialDelay.length >= 0, "Initial delay must not be negative")
      require(increaseBy > 0, "Delay increase must be greater than 0")

      override def forRetry(nbRetry: Int): Duration =
        (initialDelay.toMillis * math.pow(increaseBy, (nbRetry - 1).max(0))).toLong.milliseconds
      override def toString: String = s"${initialDelay.length} * $increaseBy"
    }
  }
}
