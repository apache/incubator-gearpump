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
package org.apache.gearpump.streaming.refactor.dsl.scalaapi.functions

import org.apache.gearpump.streaming.dsl.api.functions.{FilterFunction}
import org.apache.gearpump.streaming.dsl.scalaapi.functions.FlatMapFunction
import org.apache.gearpump.streaming.refactor.dsl.javaapi.functions.{FlatMapWithStateFunction => JFlatMapWithStateFunction}
import org.apache.gearpump.streaming.refactor.dsl.api.functions.MapWithStateFunction
import org.apache.gearpump.streaming.refactor.state.RuntimeContext

import scala.collection.JavaConverters._

object FlatMapWithStateFunction {

  def apply[T, R](fn: JFlatMapWithStateFunction[T, R]): FlatMapWithStateFunction[T, R] = {
    new FlatMapWithStateFunction[T, R] {

      override def setup(runtimeContext: RuntimeContext): Unit = {
        fn.setup(runtimeContext)
      }

      override def flatMap(t: T): TraversableOnce[R] = {
        fn.flatMap(t).asScala
      }


      override def teardown(runtimeContext: RuntimeContext): Unit = {
        fn.teardown(runtimeContext)
      }
    }
  }

//  def apply[T, R](fn: T => TraversableOnce[R]): FlatMapWithStateFunction[T, R] = {
//    new FlatMapWithStateFunction[T, R] {
//      override def flatMap(t: T): TraversableOnce[R] = {
//        fn(t)
//      }
//    }
//  }

  def apply[T, R](fn: MapWithStateFunction[T, R]): FlatMapWithStateFunction[T, R] = {
    new FlatMapWithStateFunction[T, R] {

      override def setup(runtimeContext: RuntimeContext): Unit = {
        fn.setup(runtimeContext)
      }

      override def flatMap(t: T): TraversableOnce[R] = {
        Option(fn.map(t))
      }

      override def teardown(runtimeContext: RuntimeContext): Unit = {
        fn.teardown(runtimeContext)
      }
    }
  }

  def apply[T, R](fn: FilterFunction[T]): FlatMapWithStateFunction[T, T] = {
    new FlatMapWithStateFunction[T, T] {

      override def setup(runtimeContext: RuntimeContext): Unit = {
        // TODO
        fn.setup()
      }

      override def flatMap(t: T): TraversableOnce[T] = {
        if (fn.filter(t)) {
          Option(t)
        } else {
          None
        }
      }

      override def teardown(runtimeContext: RuntimeContext): Unit = {
        // TODO
        fn.teardown()
      }
    }
  }
}

/**
 * Transforms one input into zero or more outputs of possibly different types.
 * This Scala version of FlatMapFunction returns a TraversableOnce.
 *
 * @param T Input value type
 * @param R Output value type
 */
abstract class FlatMapWithStateFunction[T, R] extends FlatMapFunction[T, R] {

  final override def setup(): Unit = {
    throw new UnsupportedOperationException("please call or override " +
      "setup(runtimeContext: RuntimeContext)")
  }

  final override def teardown(): Unit = {
    throw new UnsupportedOperationException("please call or override " +
      " teardown(runtimeContext: RuntimeContext)")
  }

  def setup(runtimeContext: RuntimeContext): Unit = {}

  def teardown(runtimeContext: RuntimeContext): Unit = {}

}
