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

package org.apache.gearpump.streaming.refactor.source

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.dsl.window.api.{WindowFunction, Windows}
import org.apache.gearpump.streaming.dsl.window.impl.Window
import org.apache.gearpump.streaming.refactor.dsl.plan.functions.DummyRunner
import org.apache.gearpump.streaming.refactor.dsl.window.impl.{DefaultWindowRunner, WindowRunner}
import org.apache.gearpump.streaming.source.DataSource
import org.apache.gearpump.streaming.{Constants, Processor}

object DataSourceProcessor {
  def apply(
      dataSource: DataSource,
      parallelism: Int = 1,
      description: String = "",
      taskConf: UserConfig = UserConfig.empty)(implicit system: ActorSystem)
    : Processor[DataSourceTask[Any, Any]] = {
    Processor[DataSourceTask[Any, Any]](parallelism, description,
      taskConf
        .withValue[DataSource](Constants.GEARPUMP_STREAMING_SOURCE, dataSource)
        .withValue[WindowRunner[Any, Any]](Constants.GEARPUMP_STREAMING_OPERATOR,
        new DefaultWindowRunner[Any, Any](
          Windows(PerElementWindowFunction, description = "perElementWindows"),
          new DummyRunner[Any])))
  }


  case object PerElementWindowFunction extends WindowFunction {
    override def apply[T](
        context: WindowFunction.Context[T]): Array[Window] = {
      Array(Window(context.timestamp, context.timestamp.plusMillis(1)))
    }

    override def isNonMerging: Boolean = true
  }
}
