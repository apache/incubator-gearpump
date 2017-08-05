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

import java.util
import java.util.Map.Entry
import java.util.{ArrayList, HashSet, List, Set}
import java.lang.Iterable

import com.google.common.collect.{HashBasedTable, Table}
import org.apache.gearpump.streaming.coder.Coder
import org.apache.gearpump.streaming.refactor.state.InMemoryGlobalStateInternals.InMemoryStateBinder
import org.apache.gearpump.streaming.refactor.state.api._

class InMemoryGlobalStateInternals[K] protected(key: K) extends StateInternals {

  protected val inMemoryStateTable: InMemoryGlobalStateInternals.StateTable =
    new InMemoryGlobalStateInternals.StateTable {
      override def binderForNamespace(namespace: StateNamespace): StateBinder = {
        new InMemoryStateBinder
      }
  }

  override def getKey: Any = key

  override def state[T <: State](namespace: StateNamespace, address: StateTag[T]): T =
    inMemoryStateTable.get(namespace, address)

}

object InMemoryGlobalStateInternals {

  abstract class StateTable {

    val stateTable: Table[StateNamespace, StateTag[_], State] = HashBasedTable.create()

    def get[StateT <: State](namespace: StateNamespace, tag: StateTag[StateT]): StateT = {
      val storage: State = stateTable.get(namespace, tag)
      if (storage != null) {
        storage.asInstanceOf[StateT]
      }

      val typedStorage: StateT = tag.getSpec.bind(tag.getId, binderForNamespace(namespace))
      stateTable.put(namespace, tag, typedStorage)
      typedStorage
    }

    def clearNamespace(namespace: StateNamespace): Unit = stateTable.rowKeySet().remove(namespace)

    def clear: Unit = stateTable.clear()

    def values: Iterable[State] = stateTable.values().asInstanceOf[Iterable[State]]

    def isNamespaceInUse(namespace: StateNamespace): Boolean = stateTable.containsRow(namespace)

    def getTagsInUse(namespace: StateNamespace): java.util.Map[StateTag[_], State]
      = stateTable.row(namespace)

    def getNamespacesInUse(): java.util.Set[StateNamespace] = stateTable.rowKeySet()

    def binderForNamespace(namespace: StateNamespace): StateBinder

  }

  class InMemoryStateBinder extends StateBinder {

    override def bindValue[T](id: String, spec: StateSpec[ValueState[T]],
        coder: Coder[T]): ValueState[T] = new InMemoryValueState[T]()

    override def bindBag[T](id: String, spec: StateSpec[BagState[T]],
        elemCoder: Coder[T]): BagState[T] = new InMemoryBagState[T]()

    override def bindSet[T](id: String, spec: StateSpec[SetState[T]],
        elemCoder: Coder[T]): SetState[T] = new InMemorySetState[T]()

    override def bindMap[KeyT, ValueT](id: String, spec: StateSpec[MapState[KeyT, ValueT]],
        mapKeyCoder: Coder[KeyT], mapValueCoder: Coder[ValueT]): MapState[KeyT, ValueT] =
      new InMemoryMapState[KeyT, ValueT]()
  }

  trait InMemoryState[T <: InMemoryState[T]] {

    def isCleared: Boolean

    def copy: T

  }

  class InMemoryBagState[T] extends BagState[T] with InMemoryState[InMemoryBagState[T]] {

    private var contents: List[T] = new ArrayList[T]

    override def readLater: BagState[T] = this

    override def isCleared: Boolean = contents.isEmpty

    override def copy: InMemoryBagState[T] = {
      val that: InMemoryBagState[T] = new InMemoryBagState[T]
      that.contents.addAll(this.contents)
      that
    }

    override def add(value: T): Unit = contents.add(value)

    override def isEmpty: ReadableState[Boolean] = {
      new ReadableState[Boolean] {
        override def readLater: ReadableState[Boolean] = {
          this
        }

        override def read: Boolean = {
          contents.isEmpty
        }
      }
    }

    override def clear: Unit = contents = new ArrayList[T]

    override def read: Iterable[T] = contents.asInstanceOf[Iterable[T]]

  }

  class InMemoryValueState[T] extends ValueState[T] with InMemoryState[InMemoryValueState[T]] {

    private var cleared: Boolean = true
    private var value: T = _

    def write(input: T): Unit = {
      cleared = false
      this.value = input
    }

    def readLater: InMemoryValueState[T] = this

    def isCleared: Boolean = cleared

    def copy: InMemoryValueState[T] = {
      val that: InMemoryValueState[T] = new InMemoryValueState[T]
      if (!this.cleared) {
        that.cleared = this.cleared
        that.value = this.value
      }

      that
    }

    def clear: Unit = {
      value = null.asInstanceOf[T]
      cleared = true
    }

    def read: T = value

  }

  class InMemoryMapState[K, V] extends MapState[K, V] with InMemoryState[InMemoryMapState[K, V]] {

    private var contents: util.Map[K, V] = new util.HashMap[K, V]

    override def put(key: K, value: V): Unit = contents.put(key, value)

    override def putIfAbsent(key: K, value: V): ReadableState[V] = {
      var v: V = contents.get(key)
      if (v == null) {
        v = contents.put(key, value)
      }

      ReadableStates.immediate(v)
    }

    override def remove(key: K): Unit = contents.remove(key)

    override def get(key: K): ReadableState[V] = ReadableStates.immediate(contents.get(key))

    override def keys: ReadableState[Iterable[K]] =
      ReadableStates.immediate(contents.keySet().asInstanceOf[Iterable[K]])

    override def values: ReadableState[Iterable[V]] =
      ReadableStates.immediate(contents.values().asInstanceOf[Iterable[V]])

    override def entries: ReadableState[Iterable[Entry[K, V]]] =
      ReadableStates.immediate(contents.entrySet().asInstanceOf[Iterable[util.Map.Entry[K, V]]])

    override def isCleared: Boolean = contents.isEmpty

    override def copy: InMemoryMapState[K, V] = {
      val that: InMemoryMapState[K, V] = new InMemoryMapState
      that.contents.putAll(this.contents)
      that
    }

    override def clear: Unit = contents = new util.HashMap[K, V]

  }

  class InMemorySetState[T] extends SetState[T] with InMemoryState[InMemorySetState[T]] {

    private var contents: Set[T] = new HashSet[T]

    override def contains(t: T): ReadableState[Boolean] =
      ReadableStates.immediate(contents.contains(t))

    override def addIfAbsent(t: T): ReadableState[Boolean] = {
      val alreadyContained: Boolean = contents.contains(t)
      contents.add(t)
      ReadableStates.immediate(!alreadyContained)
    }

    override def remove(t: T): Unit = contents.remove(t)

    override def readLater: SetState[T] = this

    override def isCleared: Boolean = contents.isEmpty

    override def copy: InMemorySetState[T] = {
      val that: InMemorySetState[T] = new InMemorySetState[T]
      that.contents.addAll(this.contents)
      that
    }

    override def add(value: T): Unit = contents.add(value)

    override def isEmpty: ReadableState[Boolean] = {
      new ReadableState[Boolean] {

        override def readLater: ReadableState[Boolean] = this

        override def read: Boolean = contents.isEmpty
      }
    }

    override def clear: Unit = contents = new HashSet[T]

    override def read: Iterable[T] = contents.asInstanceOf[Iterable[T]]
  }

}

object ReadableStates {

  def immediate[T](value: T): ReadableState[T] = {
    new ReadableState[T] {
      override def readLater: ReadableState[T] = {
        this
      }

      override def read: T = {
        value
      }
    }
  }

}
