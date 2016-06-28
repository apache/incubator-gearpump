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
package org.apache.gearpump.streaming.redis

import java.util.Arrays

import org.apache.gearpump.streaming.source.DataSource
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.LogUtil
import org.apache.gearpump.{Message, TimeStamp}
import redis.clients.jedis.Client
import redis.clients.jedis.Protocol.Keyword.{MESSAGE, PMESSAGE, SUBSCRIBE}
import redis.clients.jedis.Protocol.{DEFAULT_HOST, DEFAULT_PORT, DEFAULT_TIMEOUT}
import redis.clients.util.SafeEncoder

/**
  * Reading messages from Redis Pub/Sub Channel
  * When patterned is set to true , RedisSource will fetch message from a series channels match the channel patters
  *
  * @param host
  * @param port
  * @param timeout
  * @param patterned
  * @param channels
  */
class RedisSource(
                   host: String = DEFAULT_HOST,
                   port: Int = DEFAULT_PORT,
                   timeout: Int = DEFAULT_TIMEOUT,
                   patterned: Boolean = false,
                   channels: Array[String]) extends DataSource {

  def this(channel: String) = this(channels = Array(channel))

  private val LOG = LogUtil.getLogger(getClass)
  @transient private lazy val client = new Client(host, port)

  override def open(context: TaskContext, startTime: Long): Unit = {
    client.setTimeoutInfinite()

    if (!patterned) {
      for (channel <- channels) {
        client.subscribe(channel)
      }
    } else {
      for (channel <- channels) {
        client.psubscribe(channel)
      }
    }

    val reply = client.getObjectMultiBulkReply()
    val response = reply.get(0).asInstanceOf[Array[Byte]]

    if (Arrays.equals(SUBSCRIBE.raw, response)) {
      val channel = SafeEncoder.encode(reply.get(1).asInstanceOf[Array[Byte]]);
      LOG.info(s"Subscribed on ${channel}")
    }
  }

  override def read(): Message = {
    val reply = client.getRawObjectMultiBulkReply()
    val response = reply.get(0).asInstanceOf[Array[Byte]];

    val msg: Option[String] = if (Arrays.equals(MESSAGE.raw, response)) {
      Some(SafeEncoder.encode(reply.get(2).asInstanceOf[Array[Byte]]))
    } else if (Arrays.equals(PMESSAGE.raw, response)) {
      Some(SafeEncoder.encode(reply.get(3).asInstanceOf[Array[Byte]]))
    } else {
      None
    }

    LOG.info(s"Message ${msg.get}")
    new Message(msg, current())
  }

  private def current(): TimeStamp = System.currentTimeMillis()

  override def close(): Unit = {
    client.close()
  }
}
