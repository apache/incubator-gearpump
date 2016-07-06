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

import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.core.{ConsistencyLevel, Statement, WriteType}

class MultipleRetryPolicy(maxRetryCount: Int, retryDelay: CassandraConnectorConf.RetryDelayConf)
  extends RetryPolicy {

  private def retryOrThrow(cl: ConsistencyLevel, nbRetry: Int): RetryDecision = {
    if (nbRetry < maxRetryCount) {
      if (nbRetry > 0) {
        val delay = retryDelay.forRetry(nbRetry).toMillis
        if (delay > 0) Thread.sleep(delay)
      }
      RetryDecision.retry(cl)
    } else {
      RetryDecision.rethrow()
    }
  }

  override def init(cluster: com.datastax.driver.core.Cluster): Unit = {}
  override def close(): Unit = { }

  override def onReadTimeout(
      stmt: Statement,
      cl: ConsistencyLevel,
      requiredResponses: Int,
      receivedResponses: Int,
      dataRetrieved: Boolean,
      nbRetry: Int): RetryDecision = retryOrThrow(cl, nbRetry)

  override def onUnavailable(
      stmt: Statement,
      cl: ConsistencyLevel,
      requiredReplica: Int,
      aliveReplica: Int,
      nbRetry: Int): RetryDecision = retryOrThrow(cl, nbRetry)

  override def onWriteTimeout(
      stmt: Statement,
      cl: ConsistencyLevel,
      writeType: WriteType,
      requiredAcks: Int,
      receivedAcks: Int,
      nbRetry: Int): RetryDecision = retryOrThrow(cl, nbRetry)

  override def onRequestError(
      stmt: Statement,
      cl: ConsistencyLevel,
      ex: DriverException,
      nbRetry: Int): RetryDecision = retryOrThrow(cl, nbRetry)
}
