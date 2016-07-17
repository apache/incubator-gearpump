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

import java.net.InetAddress

import scala.concurrent.duration.{Duration, _}
import scala.util.Try

import com.datastax.driver.core.ProtocolOptions
import org.apache.gearpump.experiments.cassandra.lib.CassandraConnectorConf._

case class CassandraConnectorConf(
    hosts: Set[InetAddress] = Set(hostDefault),
    port: Int = portDefault,
    authConf: AuthConf = NoAuthConf,
    minReconnectionDelayMillis: Int = minReconnectionDelayMillisDefault,
    maxReconnectionDelayMillis: Int = maxReconnectionDelayMillisDefault,
    compression: ProtocolOptions.Compression = compressionDefault,
    queryRetryCount: Int = queryRetryCountDefault,
    connectTimeoutMillis: Int = connectTimeoutMillisDefault,
    readTimeoutMillis: Int = readTimeoutMillisDefault,
    connectionFactory: CassandraConnectionFactory = DefaultConnectionFactory,
    cassandraSSLConf: CassandraConnectorConf.CassandraSSLConf = cassandraSslConfDefault,
    queryRetryDelay: CassandraConnectorConf.RetryDelayConf = queryRetryDelayParamDefault
)

object CassandraConnectorConf {
  val hostDefault = InetAddress.getLocalHost
  val portDefault = 9042

  val keepAliveMillisDefault = 5000
  val minReconnectionDelayMillisDefault = 1000
  val maxReconnectionDelayMillisDefault = 6000
  val compressionDefault = ProtocolOptions.Compression.NONE
  val queryRetryCountDefault = 10
  val connectTimeoutMillisDefault = 5000
  val readTimeoutMillisDefault = 120000

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

    private val ConstantDelayEx = """(\d+)""".r
    private val LinearDelayEx = """(\d+)\+(.+)""".r
    private val ExponentialDelayEx = """(\d+)\*(.+)""".r

    def fromString(s: String): Option[RetryDelayConf] = s.trim match {
      case "" => None

      case ConstantDelayEx(delayStr) =>
        val d = for (delay <- Try(delayStr.toInt)) yield ConstantDelay(delay.milliseconds)
        d.toOption.orElse(throw new IllegalArgumentException(
          s"Invalid format of constant delay: $s; it should be <integer number>."))

      case LinearDelayEx(delayStr, increaseStr) =>
        val d = for (delay <- Try(delayStr.toInt); increaseBy <- Try(increaseStr.toInt))
          yield LinearDelay(delay.milliseconds, increaseBy.milliseconds)
        d.toOption.orElse(
          throw new IllegalArgumentException(
            s"Invalid format of linearly increasing delay: $s; " +
              s"it should be <integer number>+<integer number>"))

      case ExponentialDelayEx(delayStr, increaseStr) =>
        val d = for (delay <- Try(delayStr.toInt); increaseBy <- Try(increaseStr.toDouble))
          yield ExponentialDelay(delay.milliseconds, increaseBy)
        d.toOption.orElse(
          throw new IllegalArgumentException(
            s"Invalid format of exponentially increasing delay: $s; " +
              s"it should be <integer number>*<real number>"))
    }
  }

  val cassandraSslConfDefault = CassandraSSLConf()

  val queryRetryDelayParamDefault = RetryDelayConf.ExponentialDelay(4.seconds, 1.5d)

}
