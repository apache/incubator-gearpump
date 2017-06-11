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

import java.time.Instant

import org.apache.gearpump.{MAX_TIME_MILLIS, MIN_TIME_MILLIS, Message}

/**
 * message used by source task to report source watermark.
 */
case class Watermark(instant: Instant) {
  def toMessage: Message = Message("watermark", instant)
}

object Watermark {

  val MAX: Instant = Instant.ofEpochMilli(MAX_TIME_MILLIS + 1)

  val MIN: Instant = Instant.ofEpochMilli(MIN_TIME_MILLIS)
}
