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

import java.io.FileInputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{SSLContext, TrustManagerFactory}

import com.datastax.driver.core.policies.{ExponentialReconnectionPolicy, RoundRobinPolicy}
import com.datastax.driver.core.{Cluster, JdkSSLOptions, SSLOptions, SocketOptions}
import CassandraConnectorConf.CassandraSSLConf

trait CassandraConnectionFactory extends Serializable {
  def createCluster(conf: CassandraConnectorConf): Cluster
  def properties: Set[String] = Set.empty
}

object DefaultConnectionFactory extends CassandraConnectionFactory {

  def clusterBuilder(conf: CassandraConnectorConf): Cluster.Builder = {
    val options = new SocketOptions()
      .setConnectTimeoutMillis(conf.connectTimeoutMillis)
      .setReadTimeoutMillis(conf.readTimeoutMillis)

    val builder = Cluster.builder()
      .addContactPoints(conf.hosts.toSeq: _*)
      .withPort(conf.port)
      .withRetryPolicy(
        new MultipleRetryPolicy(conf.queryRetryCount, conf.queryRetryDelay))
      .withReconnectionPolicy(
        new ExponentialReconnectionPolicy(
          conf.minReconnectionDelayMillis,
          conf.maxReconnectionDelayMillis))
      .withLoadBalancingPolicy(new RoundRobinPolicy())
      .withAuthProvider(conf.authConf.authProvider)
      .withSocketOptions(options)
      .withCompression(conf.compression)

    if (conf.cassandraSSLConf.enabled) {
      maybeCreateSSLOptions(conf.cassandraSSLConf) match {
        case Some(sslOptions) => builder.withSSL(sslOptions)
        case None => builder.withSSL()
      }
    } else {
      builder
    }
  }

  private def maybeCreateSSLOptions(conf: CassandraSSLConf): Option[SSLOptions] = {
    conf.trustStorePath map {
      case path =>

        val trustStoreFile = new FileInputStream(path)
        val tmf = try {
          val keyStore = KeyStore.getInstance(conf.trustStoreType)
          conf.trustStorePassword match {
            case None => keyStore.load(trustStoreFile, null)
            case Some(password) => keyStore.load(trustStoreFile, password.toCharArray)
          }
          val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
          tmf.init(keyStore)
          tmf
        } finally {
          trustStoreFile.close()
        }

        val context = SSLContext.getInstance(conf.protocol)
        context.init(null, tmf.getTrustManagers, new SecureRandom)
        JdkSSLOptions.builder()
          .withSSLContext(context)
          .withCipherSuites(conf.enabledAlgorithms.toArray)
          .build()
    }
  }

  override def createCluster(conf: CassandraConnectorConf): Cluster = {
    clusterBuilder(conf).build()
  }
}
