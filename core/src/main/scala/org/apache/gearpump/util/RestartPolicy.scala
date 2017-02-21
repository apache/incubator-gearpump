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

package org.apache.gearpump.util

import scala.concurrent.duration.Duration

import akka.actor.ChildRestartStats

/**
 * When one executor or task fails, Gearpump will try to start. However, if it fails after
 * multiple retries, then we abort.
 *
 * @param totalNrOfRetries The total number of times is allowed to be restarted, negative value
 *                         means no limit, if the limit is exceeded the policy will not allow
 *                         to restart
 * @param maxNrOfRetriesInRange The maximum retry times in the specified time window.
 * @param withinTimeRange Duration of the time window for maxNrOfRetries.
 *                        Duration.Inf means no window
 */
class RestartPolicy(totalNrOfRetries: Int, maxNrOfRetriesInRange: Int, withinTimeRange: Duration) {
  private var historicalRetries: Int = 0
  private val status = new ChildRestartStats(null, 0, 0L)
  private val retriesWindow = (Some(maxNrOfRetriesInRange), Some(withinTimeRange.toMillis.toInt))

  def allowRestart: Boolean = {
    historicalRetries += 1
    if (totalNrOfRetries > 0 && historicalRetries > totalNrOfRetries) {
      false
    } else {
      status.requestRestartPermission(retriesWindow)
    }
  }
}
