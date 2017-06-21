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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.lang.Iterable
import java.util
import java.util.Map.Entry
import java.util._
import java.util.Objects

import com.google.common.collect.Table
import org.apache.gearpump.streaming.refactor.coder.{Coder, ListCoder, MapCoder, SetCoder}
import org.apache.gearpump.streaming.refactor.state.{StateBinder, StateNamespace, StateSpec, StateTag}
import org.apache.gearpump.streaming.refactor.state.api._
import org.apache.gearpump.util.LogUtil

/**
 *  a heap memory backend StateInternals implementation
 *  it will cache state in heap memory with a Guava's Table structure
 *  there are three concept :
 *  (1) namespace mapping table's row id
 *  (2) tag mapping table's field
 *  (3) state mapping table's cell value
 */
class HeapStateInternals[K](key: K, stateTable: Table[String, String, Array[Byte]])
  extends StateInternals {

  val LOG = LogUtil.getLogger(getClass)

  private val k: K = key
  private val st: Table[String, String, Array[Byte]] = stateTable

  private class HeapStateBinder(namespace: StateNamespace, address: StateTag[_])
    extends StateBinder {

    private val ns: StateNamespace = namespace
    private val addr: StateTag[_] = address

    override def bindValue[T](id: String, spec: StateSpec[ValueState[T]],
        coder: Coder[T]): ValueState[T] =
      new HeapValueState[T](ns, addr.asInstanceOf[StateTag[ValueState[T]]], coder)

    override def bindBag[T](id: String, spec: StateSpec[BagState[T]],
        elemCoder: Coder[T]): BagState[T] =
      new HeapBagState[T](ns, addr.asInstanceOf[StateTag[BagState[T]]], elemCoder)

    override def bindSet[T](id: String, spec: StateSpec[SetState[T]],
        elemCoder: Coder[T]): SetState[T] =
      new HeapSetState[T](ns, addr.asInstanceOf[StateTag[SetState[T]]], elemCoder)

    override def bindMap[KeyT, ValueT](id: String, spec: StateSpec[MapState[KeyT, ValueT]],
        mapKeyCoder: Coder[KeyT], mapValueCoder: Coder[ValueT]): MapState[KeyT, ValueT] =
      new HeapMapState[KeyT, ValueT](ns,
        addr.asInstanceOf[StateTag[MapState[KeyT, ValueT]]], mapKeyCoder, mapValueCoder)

  }

  override def getKey: Any = k

  override def state[T <: State](namespace: StateNamespace, address: StateTag[T]): T =
    address.bind(new HeapStateBinder(namespace, address))

  private class AbstractState[T](namespace: StateNamespace, address: StateTag[_ <: State],
      coder: Coder[T]) {

    protected val ns: StateNamespace = namespace
    protected val addr: StateTag[_ <: State] = address
    protected val c: Coder[T] = coder

    protected def readValue: T = {
      var value: T = null.asInstanceOf[T]
      val buf: Array[Byte] = st.get(ns.stringKey, addr.getId)
      if (buf != null) {
        val is: InputStream = new ByteArrayInputStream(buf)
        try {
          value = c.decode(is)
        } catch {
          case ex: unchecked => throw new RuntimeException(ex)
        }
      }

      value
    }

    def writeValue(input: T): Unit = {
      val output: ByteArrayOutputStream = new ByteArrayOutputStream();
      try {
        c.encode(input, output)
        st.put(ns.stringKey, addr.getId, output.toByteArray)
      } catch {
        case ex: unchecked => throw new RuntimeException(ex)
      }
    }

    def clear: Unit = st.remove(ns.stringKey, addr.getId)

    override def hashCode(): Int = Objects.hash(ns, addr)

    override def equals(obj: Any): Boolean = {
      if (obj == this) true

      if (null == obj || getClass != obj.getClass) false

      val that: AbstractState[_] = obj.asInstanceOf[AbstractState[_]]
      Objects.equals(ns, that.ns) && Objects.equals(addr, that.addr)
    }
  }

  private class HeapValueState[T](namespace: StateNamespace,
      address: StateTag[ValueState[T]], coder: Coder[T])
      extends AbstractState[T](namespace, address, coder) with ValueState[T] {

    override def write(input: T): Unit = writeValue(input)

    override def readLater: ValueState[T] = this

    override def read: T = readValue
  }

  private class HeapMapState[MapKT, MapVT](namespace: StateNamespace,
      address: StateTag[MapState[MapKT, MapVT]], mapKCoder: Coder[MapKT], mapVCoder: Coder[MapVT])
      extends AbstractState[Map[MapKT, MapVT]](
        namespace, address, MapCoder.of(mapKCoder, mapVCoder))
      with MapState[MapKT, MapVT] {

    private def readMap: Map[MapKT, MapVT] = {
      implicit var map = super.readValue
      if (map == null || map.size() == 0) {
        map = new util.HashMap[MapKT, MapVT]
      }

      map
    }

    override def put(key: MapKT, value: MapVT): Unit = {
      implicit var map = readMap
      map.put(key, value)
      super.writeValue(map)
    }

    override def putIfAbsent(key: MapKT, value: MapVT): ReadableState[MapVT] = {
      implicit var map = readMap
      implicit val previousVal = map.putIfAbsent(key, value)
      super.writeValue(map)
      new ReadableState[MapVT] {

        override def readLater: ReadableState[MapVT] = this

        override def read: MapVT = previousVal
      }
    }

    override def remove(key: MapKT): Unit = {
      implicit var map = readMap
      map.remove(key)
      super.writeValue(map)
    }

    override def get(key: MapKT): ReadableState[MapVT] = {
      implicit var map = readMap
      new ReadableState[MapVT] {

        override def read: MapVT = map.get(key)

        override def readLater: ReadableState[MapVT] = this
      }
    }

    override def keys: ReadableState[Iterable[MapKT]] = {
      implicit val map = readMap
      new ReadableState[Iterable[MapKT]] {

        override def readLater: ReadableState[Iterable[MapKT]] = this

        override def read: Iterable[MapKT] = map.keySet()
      }
    }

    override def values: ReadableState[Iterable[MapVT]] = {
      implicit val map = readMap
      new ReadableState[Iterable[MapVT]] {

        override def readLater: ReadableState[Iterable[MapVT]] = this

        override def read: Iterable[MapVT] = map.values()
      }
    }

    override def entries: ReadableState[Iterable[Entry[MapKT, MapVT]]] = {
      implicit var map = readMap
      new ReadableState[Iterable[Entry[MapKT, MapVT]]] {

        override def readLater: ReadableState[Iterable[Entry[MapKT, MapVT]]] = this

        override def read: Iterable[Entry[MapKT, MapVT]] = map.entrySet()
      }
    }

    override def clear: Unit = {
      implicit var map = readMap
      map.clear()
      super.writeValue(map)
    }
}

  private class HeapBagState[T](namespace: StateNamespace,
      address: StateTag[BagState[T]], coder: Coder[T])
      extends AbstractState[List[T]](namespace, address, ListCoder.of(coder)) with BagState[T] {

    override def readLater: BagState[T] = this

    override def add(input: T): Unit = {
      val value: List[T] = read
      value.add(input)
      writeValue(value)
    }

    override def isEmpty: ReadableState[Boolean] = {
      new ReadableState[Boolean] {

        override def readLater: ReadableState[Boolean] = this

        override def read: Boolean = stateTable.get(ns.stringKey, addr.getId) == null
      }
    }

    override def read: List[T] = {
      var value: List[T] = super.readValue
      if (value == null || value.size() == 0) {
        value = new ArrayList[T]
      }

      value
    }
  }

  private class HeapSetState[T](namespace: StateNamespace,
      address: StateTag[SetState[T]], coder: Coder[T])
      extends AbstractState[Set[T]](namespace, address, SetCoder.of(coder)) with SetState[T] {

    override def contains(t: T): ReadableState[Boolean] = {
      implicit val set = read
      new ReadableState[Boolean] {

        override def readLater: ReadableState[Boolean] = this

        override def read: Boolean = set.contains(t)
      }
    }

    override def addIfAbsent(t: T): ReadableState[Boolean] = {
      implicit val set = read
      val success = set.add(t)
      super.writeValue(set)
      new ReadableState[Boolean] {

        override def readLater: ReadableState[Boolean] = this

        override def read: Boolean = success
      }
    }

    override def remove(t: T): Unit = {
      implicit var set = read
      set.remove(t)
      writeValue(set)
    }

    override def readLater: SetState[T] = this

    override def add(value: T): Unit = {
      implicit var set = read
      set.add(value)
      writeValue(set)
    }

    override def isEmpty: ReadableState[Boolean] = {
      implicit val set = read
      new ReadableState[Boolean] {

        override def readLater: ReadableState[Boolean] = this

        override def read: Boolean = set.isEmpty
      }
    }

    override def read: Set[T] = {
      var value: Set[T] = super.readValue
      if (value == null || value.size() == 0) {
        value = new util.HashSet[T]()
      }

      value
    }
  }

}


