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
package org.apache.gearpump.streaming.example.redis

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.embedded.EmbeddedCluster
import org.apache.gearpump.cluster.main.ArgumentsParser
import org.apache.gearpump.streaming.redis.{RedisSource, RedisStorage}
import org.apache.gearpump.streaming.sink.DataSinkProcessor
import org.apache.gearpump.streaming.source.DataSourceProcessor
import org.apache.gearpump.streaming.task.{Task, TaskContext}
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph._
import org.apache.gearpump.util.{AkkaApp, Graph}

class UpperProcessor(taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  import taskContext.output

  override def onNext(message: Message): Unit = {
    Option(message.msg) match {
      case Some(msg) =>
        val upper = msg.asInstanceOf[String].toUpperCase
        LOG.info(s"to Upper $upper")
        output(new Message(upper, message.timestamp))
      case None => LOG.warn("Empty String")
    }
  }
}

object RedisSourceStorageExample extends AkkaApp with ArgumentsParser {
  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val cluster = new EmbeddedCluster(akkaConf: Config)
    cluster.start()

    val context = cluster.newClientContext
    implicit val actorSystem = context.system

    val source = DataSourceProcessor(new RedisSource(channel = "channel.in"), 1)
    val upper = Processor[UpperProcessor](1)
    val sink = DataSinkProcessor(new RedisStorage(), 1)
    val dag = source ~> upper ~> sink
    val app = StreamApplication("RedisSourceStorage", Graph(dag), UserConfig.empty)

    context.submit(app)
    Thread.sleep(600 * 1000)
    context.close()
    cluster.stop()
  }
}
