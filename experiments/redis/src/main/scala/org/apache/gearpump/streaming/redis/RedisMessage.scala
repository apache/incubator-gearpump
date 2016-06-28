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

import java.nio.charset.Charset

object RedisMessage {

  private def toBytes(string: String,
                      charset: Charset = Charset.forName("UTF8")
                     ): Array[Byte] = string.getBytes(charset)

  /**
    * Send message to a Redis Channel
    *
    * @param message
    */
  case class PublishMessage(message: Array[Byte]) {
    def this(message: String) = this(toBytes(message))
  }

  /**
    * Set the value for a key
    *
    * @param key
    * @param value
    */
  case class SetMessage(key: Array[Byte], value: Array[Byte]) {
    def this(key: String, value: String) = this(toBytes(key), toBytes(value))
  }

  /**
    * Set the value at the head of a list
    *
    * @param key
    * @param value
    */
  case class LPushMessage(key: Array[Byte], value: Array[Byte]) {
    def this(key: String, value: String) = this(toBytes(key), toBytes(value))
  }

  /**
    * Set the value at the tail of a list
    *
    * @param key
    * @param value
    */
  case class RPushMessage(key: Array[Byte], value: Array[Byte]) {
    def this(key: String, value: String) = this(toBytes(key), toBytes(value))
  }

  /**
    * Set the value as a field of a hash
    *
    * @param key
    * @param field
    * @param value
    */
  case class HSetMessage(key: Array[Byte], field: Array[Byte], value: Array[Byte]) {
    def this(key: String, field: String, value: String) = this(toBytes(key), toBytes(field), toBytes(value))
  }

  /**
    * Set the value to a set
    *
    * @param key
    * @param value
    */
  case class SAddMessage(key: Array[Byte], value: Array[Byte]) {
    def this(key: String, value: String) = this(toBytes(key), toBytes(value))
  }

  /**
    * Set the value to a sorted set
    *
    * @param key
    * @param score
    * @param value
    */
  case class ZAddMessage(key: Array[Byte], score: Double, value: Array[Byte]) {
    def this(key: String, score: Double, value: String) = this(toBytes(key), score, toBytes(value))
  }

  /**
    * Set the value to the HyperLogLog data structure
    *
    * @param key
    * @param value
    */
  case class PFAdd(key: Array[Byte], value: Array[Byte]) {
    def this(key: String, value: String) = this(toBytes(key), toBytes(value))
  }

  /**
    * Set the geospatial information to the specified key
    *
    * @param key
    * @param longitude
    * @param latitude
    * @param value
    */
  case class GEOAdd(key: Array[Byte], longitude: Double, latitude: Double, value: Array[Byte]) {
    def this(key: String, longitude: Double, latitude: Double, value: String) =
      this(toBytes(key), longitude, latitude, toBytes(value))
  }

}
