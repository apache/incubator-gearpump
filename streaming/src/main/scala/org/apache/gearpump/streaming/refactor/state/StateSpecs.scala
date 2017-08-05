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

import java.util.Objects

import org.apache.gearpump.streaming.coder.Coder
import org.apache.gearpump.streaming.refactor.state.api.{BagState, MapState, SetState, ValueState}

object StateSpecs {

  private class ValueStateSpec[T](coder: Coder[T]) extends StateSpec[ValueState[T]] {

    var aCoder: Coder[T] = coder

    override def bind(id: String, binder: StateBinder): ValueState[T] = {
      binder.bindValue(id, this, aCoder)
    }

    override def offerCoders(coders: Array[Coder[ValueState[T]]]): Unit = {
      if (this.aCoder == null) {
        if (coders(0) != null) {
          this.aCoder = coders(0).asInstanceOf[Coder[T]]
        }
      }
    }

    override def finishSpecifying: Unit = {
      if (aCoder == null) throw new IllegalStateException(
        "Unable to infer a coder for ValueState and no Coder"
        + " was specified. Please set a coder by either invoking"
        + " StateSpecs.value(Coder<T> valueCoder) or by registering the coder in the"
        + " Pipeline's CoderRegistry.")
    }

    override def equals(obj: Any): Boolean = {
      var result = false
      if (obj == this) result = true

      if (!(obj.isInstanceOf[ValueStateSpec[T]])) result = false

      val that: ValueStateSpec[_] = obj.asInstanceOf[ValueStateSpec[_]]
      result = Objects.equals(this.aCoder, that.aCoder)
      result
    }

    override def hashCode(): Int = {
      Objects.hashCode(this.aCoder)
    }
  }

  private class BagStateSpec[T](coder: Coder[T]) extends StateSpec[BagState[T]] {

    private implicit var elemCoder = coder

    override def bind(id: String, binder: StateBinder): BagState[T] =
      binder.bindBag(id, this, elemCoder)

    override def offerCoders(coders: Array[Coder[BagState[T]]]): Unit = {
      if (this.elemCoder == null) {
        if (coders(0) != null) {
          this.elemCoder = coders(0).asInstanceOf[Coder[T]]
        }
      }
    }

    override def finishSpecifying: Unit = {
      if (elemCoder == null) {
        throw new IllegalStateException("Unable to infer a coder for BagState and no Coder"
          + " was specified. Please set a coder by either invoking"
          + " StateSpecs.bag(Coder<T> elemCoder) or by registering the coder in the"
          + " Pipeline's CoderRegistry.");
      }
    }

    override def equals(obj: Any): Boolean = {
      var result = false
      if (obj == this) result = true

      if (!obj.isInstanceOf[BagStateSpec[_]]) result = false

      val that = obj.asInstanceOf[BagStateSpec[_]]
      result = Objects.equals(this.elemCoder, that.elemCoder)
      result
    }

    override def hashCode(): Int = Objects.hash(getClass, elemCoder)
  }

  private class MapStateSpec[K, V](keyCoder: Coder[K], valueCoder: Coder[V])
    extends StateSpec[MapState[K, V]] {

    private implicit var kCoder = keyCoder
    private implicit var vCoder = valueCoder

    override def bind(id: String, binder: StateBinder): MapState[K, V] =
      binder.bindMap(id, this, keyCoder, valueCoder)

    override def offerCoders(coders: Array[Coder[MapState[K, V]]]): Unit = {
      if (this.kCoder == null) {
        if (coders(0) != null) {
          this.kCoder = coders(0).asInstanceOf[Coder[K]]
        }
      }

      if (this.vCoder == null) {
        if (coders(1) != null) {
          this.vCoder = coders(1).asInstanceOf[Coder[V]]
        }
      }
    }

    override def finishSpecifying: Unit = {
      if (keyCoder == null || valueCoder == null) {
        throw new IllegalStateException("Unable to infer a coder for MapState and no Coder"
          + " was specified. Please set a coder by either invoking"
          + " StateSpecs.map(Coder<K> keyCoder, Coder<V> valueCoder) or by registering the"
          + " coder in the Pipeline's CoderRegistry.");
      }
    }

    override def hashCode(): Int = Objects.hash(getClass, kCoder, vCoder)

    override def equals(obj: Any): Boolean = {
      var result = false
      if (obj == this) result = true

      if (!obj.isInstanceOf[MapStateSpec[_, _]]) result = false

      implicit var that = obj.asInstanceOf[MapStateSpec[_, _]]
      result = Objects.equals(this.kCoder, that.vCoder) && Objects.equals(this.vCoder, that.vCoder)
      result
    }
  }

  private class SetStateSpec[T](coder: Coder[T]) extends StateSpec[SetState[T]] {

    private implicit var elemCoder = coder

    override def bind(id: String, binder: StateBinder): SetState[T] =
      binder.bindSet(id, this, elemCoder)

    override def offerCoders(coders: Array[Coder[SetState[T]]]): Unit = {
      if (this.elemCoder == null) {
        if (coders(0) != null) {
          this.elemCoder = coders(0).asInstanceOf[Coder[T]]
        }
      }
    }

    override def finishSpecifying: Unit = {
      if (elemCoder == null) {
        throw new IllegalStateException("Unable to infer a coder for SetState and no Coder"
          + " was specified. Please set a coder by either invoking"
          + " StateSpecs.set(Coder<T> elemCoder) or by registering the coder in the"
          + " Pipeline's CoderRegistry.");
      }
    }

    override def equals(obj: Any): Boolean = {
      var result = false
      if (obj == this) result = true

      if (!obj.isInstanceOf[SetStateSpec[_]]) result = false

      implicit var that = obj.asInstanceOf[SetStateSpec[_]]
      result = Objects.equals(this.elemCoder, that.elemCoder)
      result
    }

    override def hashCode(): Int = Objects.hash(getClass, elemCoder)
  }

  def value[T]: StateSpec[ValueState[T]] = new ValueStateSpec[T](null)

  def value[T](valueCoder: Coder[T]): StateSpec[ValueState[T]] = {
    if (valueCoder == null) {
      throw new NullPointerException("valueCoder should not be null. Consider value() instead")
    }

    new ValueStateSpec[T](valueCoder)
  }

  def bag[T]: StateSpec[BagState[T]] = new BagStateSpec[T](null)

  def bag[T](elemCoder: Coder[T]): StateSpec[BagState[T]] = new BagStateSpec[T](elemCoder)

  def set[T]: StateSpec[SetState[T]] = new SetStateSpec[T](null)

  def set[T](elemCoder: Coder[T]): StateSpec[SetState[T]] = new SetStateSpec[T](elemCoder)

  def map[K, V]: StateSpec[MapState[K, V]] = new MapStateSpec[K, V](null, null)

  def map[K, V](keyCoder: Coder[K], valueCoder: Coder[V]): StateSpec[MapState[K, V]] =
    new MapStateSpec[K, V](keyCoder, valueCoder)

}
