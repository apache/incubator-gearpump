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

package org.apache.gearpump.streaming.examples.state.refactor

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.refactor.coder._
import org.apache.gearpump.streaming.refactor.state.api.{StateInternals, ValueState}
import org.apache.gearpump.streaming.refactor.state.{StateNamespaces, StateTags, StatefulTask}
import org.apache.gearpump.streaming.task.TaskContext

/**
 *  a sum processor for continues sum message from kafka
 *  it is a example for using state api and verifying state exactly-once guarantee
 */
class SumProcessor(taskContext: TaskContext, conf: UserConfig)
  extends StatefulTask(taskContext, conf) {

  private implicit val valueStateTag = "tag1"
  private implicit val counterStateTag = "tag2"

  private var stateInternals: Option[StateInternals] = None
  private var valueState: Option[ValueState[java.lang.Long]] = None
  private var counterState: Option[ValueState[java.lang.Long]] = None

  override def open: Unit = {
    stateInternals = Some(getStateInternals(StringUtf8Coder.of, "partitionedKey"))
    valueState = Some(
      stateInternals.get.state(
        StateNamespaces.global, StateTags.value(valueStateTag, VarLongCoder.of))
    )

    counterState = Some(
      stateInternals.get.state(
        StateNamespaces.global, StateTags.value(counterStateTag, VarLongCoder.of))
    )

    // init
    if (valueState.get.read == null) {
      LOG.info("[open] value state current is null, init it to 0")
      valueState.get.write(0L)
    } else {
      LOG.info("[open] load from snapshot value state current is : {}", valueState.get.read)
    }

    if (counterState.get.read == null) {
      LOG.info("[open] counter state current is null, init it to 0")
      counterState.get.write(0L)
    } else {
      LOG.info("[open] load from snapshot counter state current is : {}", counterState.get.read)
    }
  }

  override def invoke(message: Message): Unit = {
    message.msg match {
      case numberByte: Array[Byte] => {
        val number = new String(numberByte)
        val oldVal = valueState.get.read
        valueState.get.write(oldVal + java.lang.Long.valueOf(number))

        val oldCounter = counterState.get.read
        counterState.get.write(oldCounter + 1)
      }

      case other => LOG.error("received unsupported message {}", other)
    }

    if (counterState.get.read % 1000000 == 0) {
      LOG.info("counter state is : {}", counterState.get.read)
      LOG.info("value state is : {}", valueState.get.read)
    }
  }

  override def close: Unit = {}

}
