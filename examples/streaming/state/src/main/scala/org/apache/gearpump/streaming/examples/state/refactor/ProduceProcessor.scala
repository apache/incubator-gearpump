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
import org.apache.gearpump.streaming.refactor.state.StatefulTask
import org.apache.gearpump.streaming.task.TaskContext

/**
 *  a produce processor for generating a specific num sequence
 */
class ProduceProcessor(taskContext: TaskContext, conf: UserConfig)
  extends StatefulTask(taskContext, conf) {

  override def open: Unit = {}

  override def invoke(message: Message): Unit = {
    message.msg match {
      case msgBytes: Array[Byte] => {
        val msgStr = new String(msgBytes)
        LOG.info("got total sequence num : {}", msgStr)

        val n: Int = Integer.valueOf(msgStr)
        var sumResult: Long = 0
        for (i <- 1 to n) {
          taskContext.output(Message(String.valueOf(i).getBytes))
          sumResult = sumResult + i
        }

        LOG.info(" total sum result : {}", sumResult)
      }
    }
  }

  override def close: Unit = {}
}
