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

package org.apache.gearpump.streaming.refactor.state

import org.apache.gearpump.streaming.coder.Coder
import org.apache.gearpump.streaming.refactor.state.api.{BagState, MapState, SetState, ValueState}

trait StateBinder {

  def bindValue[T](id: String, spec: StateSpec[ValueState[T]], coder: Coder[T]): ValueState[T]

  def bindBag[T](id: String, spec: StateSpec[BagState[T]], elemCoder: Coder[T]): BagState[T]

  def bindSet[T](id: String, spec: StateSpec[SetState[T]], elemCoder: Coder[T]): SetState[T]

  def bindMap[KeyT, ValueT](id: String, spec: StateSpec[MapState[KeyT, ValueT]],
      mapKeyCoder: Coder[KeyT], mapValueCoder: Coder[ValueT]): MapState[KeyT, ValueT]

}
