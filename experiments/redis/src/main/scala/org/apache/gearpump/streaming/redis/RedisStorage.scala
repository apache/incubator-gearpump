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

import io.gearpump.google.common.base.Strings
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.redis.RedisMessage._
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.LogUtil
import redis.clients.jedis.Jedis
import redis.clients.jedis.Protocol.{DEFAULT_DATABASE, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_TIMEOUT}

/**
  * Save message in Redis Instance
  *
  * @param host
  * @param port
  * @param timeout
  * @param database
  * @param password
  */
class RedisStorage(
                    host: String = DEFAULT_HOST,
                    port: Int = DEFAULT_PORT,
                    timeout: Int = DEFAULT_TIMEOUT,
                    database: Int = DEFAULT_DATABASE,
                    password: String = "") extends DataSink {

  private val LOG = LogUtil.getLogger(getClass)
  @transient private lazy val client = new Jedis(host, port, timeout)

  override def open(context: TaskContext): Unit = {
    client.select(database)

    if (!Strings.isNullOrEmpty(password)) {
      client.auth(password)
    }
  }

  override def write(message: Message): Unit = {
    val msg = message.msg

    msg match {
      case set: SetMessage => client.set(set.key, set.value)
      case lpush: LPushMessage => client.lpush(lpush.key, lpush.value)
      case rpush: RPushMessage => client.rpush(rpush.key, rpush.value)
      case hset: HSetMessage => client.hset(hset.key, hset.field, hset.value)
      case sadd: SAddMessage => client.sadd(sadd.key, sadd.value)
      case zadd: ZAddMessage => client.zadd(zadd.key, zadd.score, zadd.value)
      case pfAdd: PFAdd => client.pfadd(pfAdd.key, pfAdd.value)
      case geoAdd: GEOAdd => client.geoadd(geoAdd.key, geoAdd.longitude, geoAdd.latitude, geoAdd.value)
    }
  }

  override def close(): Unit = {
    client.close()
  }
}
