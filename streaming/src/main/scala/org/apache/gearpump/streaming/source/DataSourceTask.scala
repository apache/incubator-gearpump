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

package org.apache.gearpump.streaming.source

import org.apache.gearpump._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{UpdateWatermark, StartTime, Task, TaskContext}

object DataSourceTask {
  val DATA_SOURCE = "data_source"
}

/**
 * Default Task container for [[org.apache.gearpump.streaming.source.DataSource]] that
 * reads from DataSource in batch
 * See [[org.apache.gearpump.streaming.source.DataSourceProcessor]] for its usage
 *
 * DataSourceTask calls:
 *  - `DataSource.open()` in `onStart` and pass in
 *  [[org.apache.gearpump.streaming.task.TaskContext]]
 * and application start time
 *  - `DataSource.read()` in each `onNext`, which reads a batch of messages
 *  - `DataSource.close()` in `onStop`
 */
class DataSourceTask(context: TaskContext, conf: UserConfig) extends Task(context, conf) {
  import org.apache.gearpump.streaming.source.DataSourceTask._

  private val source = conf.getValue[DataSource](DATA_SOURCE).get
  private val batchSize = conf.getInt(DataSourceConfig.SOURCE_READ_BATCH_SIZE).getOrElse(1000)
  private var startTime = 0L

  override def onStart(newStartTime: StartTime): Unit = {
    startTime = newStartTime.startTime
    LOG.info(s"opening data source at $startTime")
    source.open(context, startTime)
    self ! Message("start")
  }

  override def onNext(message: Message): Unit = {
    var num = 0
    0.until(batchSize).foreach { _ =>
      Option(source.read()).foreach { msg =>
        num += 1
        context.output(msg)
      }
    }
    if (num > 0) {
      context.appMaster ! UpdateWatermark(context.taskId, source.getWatermark)
    }
    self ! Message("continue")
  }

  override def onStop(): Unit = {
    LOG.info("closing data source...")
    source.close()
  }
}
