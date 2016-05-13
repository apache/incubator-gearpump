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
package io.gearpump.streaming.example.redis

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.embedded.EmbeddedCluster
import io.gearpump.cluster.main.ArgumentsParser
import io.gearpump.streaming.redis.RedisMessage.SetMessage
import io.gearpump.streaming.redis.{RedisSource, RedisStorage}
import io.gearpump.streaming.sink.DataSinkProcessor
import io.gearpump.streaming.source.DataSourceProcessor
import io.gearpump.streaming.task.{Task, TaskContext}
import io.gearpump.streaming.{Processor, StreamApplication}
import io.gearpump.util.Graph._
import io.gearpump.util.{AkkaApp, Graph}

class RedisSourceStorageUpperProcessor(taskContext: TaskContext, conf: UserConfig)
  extends Task(taskContext, conf) {

  import taskContext.output

  override def onNext(message: Message): Unit = {
    val msg = message.msg.asInstanceOf[Option[String]]

    if (!msg.isEmpty) {
      val upper = msg.get.toUpperCase
      LOG.info("to Upper : " + upper)
      output(new Message(new SetMessage(msg.get, upper), message.timestamp))
    }
  }
}

object RedisSourceStorage extends AkkaApp with ArgumentsParser {
  override def main(akkaConf: RedisSourceStorage.Config, args: Array[String]): Unit = {
    val cluster = new EmbeddedCluster(akkaConf: Config)
    cluster.start()

    val context = cluster.newClientContext
    implicit val actorSystem = context.system

    val source = DataSourceProcessor(new RedisSource(channel = "channel.in"), 1)
    val upper = Processor[RedisSourceStorageUpperProcessor](1)
    val sink = DataSinkProcessor(new RedisStorage(), 1)
    val dag = source ~> upper ~> sink
    val app = StreamApplication("RedisSourceStorage", Graph(dag), UserConfig.empty)

    context.submit(app)
    Thread.sleep(600 * 1000)
    context.close()
    cluster.stop()
  }
}
