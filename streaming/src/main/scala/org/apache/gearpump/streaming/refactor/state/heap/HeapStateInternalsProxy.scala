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

import org.apache.gearpump.streaming.refactor.coder.Coder
import org.apache.gearpump.streaming.refactor.state.{StateNamespace, StateTag}
import org.apache.gearpump.streaming.refactor.state.api.{State, StateInternals, StateInternalsFactory}

/**
 * a utility class help use HeapStateInternals
 */
class HeapStateInternalsProxy[K](heapStateInternalsFactory: HeapStateInternalsFactory[K])
  extends StateInternals with Serializable {

  private val factory: HeapStateInternalsFactory[K] = heapStateInternalsFactory

  @transient
  private var currentKey: K = _

  def getFactory: StateInternalsFactory[K] = {
    factory
  }

  def getKeyCoder: Coder[K] = {
    factory.getKeyCoder
  }

  override def getKey: K = {
    currentKey
  }

  def setKey(key: K): Unit = {
    currentKey = key
  }

  override def state[T <: State](namespace: StateNamespace, address: StateTag[T]): T = {
    factory.stateInternalsForKey(currentKey).state(namespace, address)
  }
}
