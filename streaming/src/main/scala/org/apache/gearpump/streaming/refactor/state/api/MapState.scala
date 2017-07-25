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

trait MapState[K, V] extends State {

  def put(key : K, value : V): Unit

  def putIfAbsent(key : K, value : V): ReadableState[V]

  def remove(key : K): Unit

  def get(key : K): ReadableState[V]

  def keys: ReadableState[Iterable[K]]

  def values: ReadableState[Iterable[V]]

  def entries: ReadableState[Iterable[java.util.Map.Entry[K, V]]]

}
