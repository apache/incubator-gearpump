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

package org.apache.gearpump.streaming.refactor.state

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.refactor.coder._
import org.apache.gearpump.streaming.task.TaskContext
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

/**
 * a test case for StatefulTask
 */
class StatefulTaskSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  class SpecificStatefulTask(taskContext: TaskContext, conf: UserConfig)
    extends StatefulTask(taskContext, conf) {

    override def invoke(message: Message): Unit = {}

  }

  property("test StatefulTask getStateInternals") {
    val taskContext = mock[TaskContext]
    val config = UserConfig.empty
    implicit val aStatefulTask = new SpecificStatefulTask(taskContext, config)

    implicit val stringKeyCoder = StringUtf8Coder.of
    val intKeyCoder = VarIntCoder.of
    implicit val stringKey = "key1"
    val intKey: Integer = 1

    implicit val testValue = "test"

    val stringInternals = aStatefulTask.getStateInternals(stringKeyCoder, stringKey)
    val intInternals = aStatefulTask.getStateInternals(intKeyCoder, intKey)

    val valueState = stringInternals.state(StateNamespaces.global,
      StateTags.value("subKey", StringUtf8Coder.of))

    valueState.write(testValue)

    valueState.read should be(testValue)

    val newKeyValueState = intInternals.state(StateNamespaces.global,
      StateTags.value("subKey", StringUtf8Coder.of))

    newKeyValueState.read shouldNot be(testValue)

    newKeyValueState.write("hello world")
    newKeyValueState.read shouldBe ("hello world")

  }

}
