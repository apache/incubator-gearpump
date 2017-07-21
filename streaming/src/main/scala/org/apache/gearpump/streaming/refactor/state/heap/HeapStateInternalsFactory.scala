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

package org.apache.gearpump.streaming.refactor.state.heap

import org.apache.gearpump.streaming.refactor.coder.{Coder, CoderException, CoderUtils}
import org.apache.gearpump.streaming.refactor.state.api.{StateInternals, StateInternalsFactory}
import java.util._

import com.google.common.collect.{HashBasedTable, Table}
import org.apache.gearpump.util.LogUtil

class HeapStateInternalsFactory[K](keyCoder: Coder[K],
    map: Map[String, Table[String, String, Array[Byte]]])
    extends StateInternalsFactory[K] with Serializable {

  private val LOG = LogUtil.getLogger(getClass)

  private val kc: Coder[K] = keyCoder
  private val perKeyState: Map[String, Table[String, String, Array[Byte]]] = map

  def getKeyCoder: Coder[K] = {
    this.kc
  }

  override def stateInternalsForKey(key: K): StateInternals = {
    var keyBytes: Option[Array[Byte]] = None
      if (key != null) {
        keyBytes = Some(CoderUtils.encodeToByteArray(kc, key))
      }

    if (keyBytes.isEmpty) {
      throw new RuntimeException("key bytes is null or empty, encode key occurs a error")
    }

    val keyBased64Str = Base64.getEncoder.encodeToString(keyBytes.get)
    var stateTable: Table[String, String, Array[Byte]] = perKeyState.get(keyBased64Str)
    if (stateTable == null) {
      LOG.info("stateTable is null, will create!")
      stateTable = HashBasedTable.create()
      perKeyState.put(keyBased64Str, stateTable)
    }

    new HeapStateInternals[K](key, stateTable)
  }

}
