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

package org.apache.gearpump.streaming.refactor.state.api

import java.lang.Iterable

/**
 * A {@link ReadableState} cell containing a set of elements.
 *
 * <p>Implementations of this form of state are expected to implement set operations such as {@link
 * #contains(Object)} efficiently, reading as little of the overall set as possible.
 *
 * @tparam T The type of elements in the set.
 */
trait SetState[T] extends GroupingState[T, Iterable[T]]{

  /**
   * Returns true if this set contains the specified element.
   * @param t
   * @return
   */
  def contains(t: T): ReadableState[Boolean]

  /** Removes the specified element from this set if it is present. */
  def addIfAbsent(t: T): ReadableState[Boolean]

  /** Removes the specified element from this set if it is present. */
  def remove(t: T): Unit

  def readLater: SetState[T]

}
