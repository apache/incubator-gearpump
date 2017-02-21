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

import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration._

class RestartPolicySpec extends FlatSpec with Matchers {

  "RestartPolicy" should "forbid too many restarts" in {
    val policy = new RestartPolicy(3, 10, 60.seconds)
    assert(policy.allowRestart)
    assert(policy.allowRestart)
    assert(policy.allowRestart)
    assert(!policy.allowRestart)
  }

  "RestartPolicy" should "forbid too many restarts in a window duration" in {
    val policy = new RestartPolicy(20, 3, 60.seconds)
    assert(policy.allowRestart)
    assert(policy.allowRestart)
    assert(policy.allowRestart)
    assert(!policy.allowRestart)

    val policy2 = new RestartPolicy(20, 3, 3.seconds)
    assert(policy2.allowRestart)
    assert(policy2.allowRestart)
    assert(policy2.allowRestart)
    Thread.sleep(4000)
    assert(policy2.allowRestart)
  }
}
