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

package org.apache.gearpump.streaming.refactor.dsl.api.functions

import org.apache.gearpump.streaming.dsl.api.functions.MapFunction
import org.apache.gearpump.streaming.refactor.state.RuntimeContext

object MapWithStateFunction {

  def apply[T, R](fn: T => R): MapWithStateFunction[T, R] = {
    new MapWithStateFunction[T, R] {
      override def map(t: T): R = {
        fn(t)
      }
    }
  }

}

/**
 *  map function support state
 */
abstract class MapWithStateFunction[T, R] extends MapFunction[T, R] {

  final override def setup(): Unit = {
    throw new UnsupportedOperationException("please call or override " +
      "setup(runtimeContext: RuntimeContext) .")
  }

  final override def teardown(): Unit = {
    throw new UnsupportedOperationException("please call or override " +
      "teardown(runtimeContext: RuntimeContext) ")
  }

  def setup(runtimeContext: RuntimeContext): Unit = {}

  def teardown(runtimeContext: RuntimeContext): Unit = {}

}
