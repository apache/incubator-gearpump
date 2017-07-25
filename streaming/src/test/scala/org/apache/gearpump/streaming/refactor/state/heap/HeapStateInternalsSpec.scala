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

import java.util
import java.util.{Iterator, Map}

import com.google.common.collect.Table
import org.apache.gearpump.streaming.refactor.coder.StringUtf8Coder
import org.apache.gearpump.streaming.refactor.state.api.{BagState, SetState, ValueState}
import org.apache.gearpump.streaming.refactor.state.{StateNamespaces, StateTags}
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class HeapStateInternalsSpec
  extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  property("HeapStateInternalsProxy should return correct key coder") {
    val map: Map[String, Table[String, String, Array[Byte]]]
    = new util.HashMap[String, Table[String, String, Array[Byte]]]()
    val factory: HeapStateInternalsFactory[String] =
      new HeapStateInternalsFactory[String](StringUtf8Coder.of, map)
    val proxy: HeapStateInternalsProxy[String] = new HeapStateInternalsProxy[String](factory)

    factory.getKeyCoder shouldBe StringUtf8Coder.of
  }

  // region value state
  property("test heap value state: write heap state should equals read state") {
    implicit val key = "key"
    implicit val namespace = StateNamespaces.global
    implicit val stateId = "01"
    implicit val value = "hello world"

    val map: Map[String, Table[String, String, Array[Byte]]]
    = new util.HashMap[String, Table[String, String, Array[Byte]]]()
    val factory = new HeapStateInternalsFactory[String](StringUtf8Coder.of, map)
    val stateInternals = factory.stateInternalsForKey(key)
    val valueState = stateInternals.state[ValueState[String]](namespace,
      StateTags.value(stateId, StringUtf8Coder.of))

    valueState.write(value)
    valueState.read shouldBe value
  }

  property("test heap value state: write heap state should not equals read state " +
    "for different state id") {
    implicit val key = "key"
    implicit val namespace = StateNamespaces.global
    implicit val stateId = "01"
    implicit val newStateId = "02"
    implicit val value = "hello world"

    val map: Map[String, Table[String, String, Array[Byte]]]
    = new util.HashMap[String, Table[String, String, Array[Byte]]]()
    val factory = new HeapStateInternalsFactory[String](StringUtf8Coder.of, map)
    val stateInternals = factory.stateInternalsForKey(key)
    val valueState = stateInternals.state[ValueState[String]](namespace,
      StateTags.value(stateId, StringUtf8Coder.of))

    valueState.write(value)

    val newValueState = stateInternals.state[ValueState[String]](namespace,
      StateTags.value(newStateId, StringUtf8Coder.of))
    newValueState.read shouldNot be(value)
  }

  property("test heap value state: write heap state should equals read state " +
    "for different key") {
    implicit val key = "key"
    implicit val newKey = "newKey"
    implicit val namespace = StateNamespaces.global
    implicit val stateId = "01"
    implicit val value = "hello world"

    val map: Map[String, Table[String, String, Array[Byte]]]
    = new util.HashMap[String, Table[String, String, Array[Byte]]]()
    val factory = new HeapStateInternalsFactory[String](StringUtf8Coder.of, map)

    val stateInternals = factory.stateInternalsForKey(key)
    val valueState = stateInternals.state[ValueState[String]](namespace,
      StateTags.value(stateId, StringUtf8Coder.of))

    val newStateInternals = factory.stateInternalsForKey(newKey)
    val newValueState = newStateInternals.state[ValueState[String]](namespace,
      StateTags.value(stateId, StringUtf8Coder.of))

    valueState.write(value)
    newValueState.read shouldNot be(value)
  }
  // endregion

  // region bag state
  property("test heap Bag state: write heap state should equals read state") {
    implicit val key = "key"
    implicit val namespace = StateNamespaces.global
    implicit val bagValue1 = "bagValue1"
    implicit val bagValue2 = "bagValue2"
    implicit val stateId = "01"

    val map: Map[String, Table[String, String, Array[Byte]]]
    = new util.HashMap[String, Table[String, String, Array[Byte]]]()
    val factory = new HeapStateInternalsFactory[String](StringUtf8Coder.of, map)

    val stateInternals = factory.stateInternalsForKey(key)
    val bagState = stateInternals.state[BagState[String]](namespace,
      StateTags.bag(stateId, StringUtf8Coder.of))

    bagState.add(bagValue1)
    bagState.add(bagValue2)

    val bagIterator: Iterator[String] = bagState.read.iterator()

    implicit var counter = 0

    while (bagIterator.hasNext) {
      counter += 1
      if (counter == 1) {
        bagIterator.next() shouldBe bagValue1
      }
      if (counter == 2) {
        bagIterator.next() shouldBe bagValue2
      }
    }

    counter shouldBe 2
  }

  property("test heap Bag state: write heap state should not equal read state with " +
    "different key") {
    implicit val key = "key"
    implicit val newKey = "newKey"
    implicit val namespace = StateNamespaces.global
    implicit val bagValue1 = "bagValue1"
    implicit val bagValue2 = "bagValue2"
    implicit val stateId = "01"

    val map: Map[String, Table[String, String, Array[Byte]]]
    = new util.HashMap[String, Table[String, String, Array[Byte]]]()
    val factory = new HeapStateInternalsFactory[String](StringUtf8Coder.of, map)

    val stateInternals = factory.stateInternalsForKey(key)
    val newStateInternals = factory.stateInternalsForKey(newKey)

    val bagState = stateInternals.state[BagState[String]](namespace,
      StateTags.bag(stateId, StringUtf8Coder.of))
    val newBagState = newStateInternals.state[BagState[String]](namespace,
      StateTags.bag(stateId, StringUtf8Coder.of))

    bagState.add(bagValue1)
    bagState.add(bagValue2)

    val newBagIterator: Iterator[String] = newBagState.read.iterator()

    implicit var counter = 0

    while (newBagIterator.hasNext) {
      counter += 1
      newBagIterator.next()
    }

    counter shouldBe 0
  }

  property("test heap Bag state: write heap state should not equal read state " +
    "with different stateId") {
    implicit val key = "key"
    implicit val namespace = StateNamespaces.global
    implicit val bagValue1 = "bagValue1"
    implicit val bagValue2 = "bagValue2"
    implicit val stateId = "01"
    implicit val newStateId = "02"

    val map: Map[String, Table[String, String, Array[Byte]]]
    = new util.HashMap[String, Table[String, String, Array[Byte]]]()
    val factory = new HeapStateInternalsFactory[String](StringUtf8Coder.of, map)

    val stateInternals = factory.stateInternalsForKey(key)
    val bagState = stateInternals.state[BagState[String]](namespace,
      StateTags.bag(stateId, StringUtf8Coder.of))
    val newBagState = stateInternals.state[BagState[String]](namespace,
      StateTags.bag(newStateId, StringUtf8Coder.of))

    bagState.add(bagValue1)
    bagState.add(bagValue2)

    val newBagIterator: Iterator[String] = newBagState.read.iterator()

    implicit var counter = 0

    while (newBagIterator.hasNext) {
      counter += 1
      newBagIterator.next()
    }

    counter shouldBe 0
  }
  // endregion

  // region set state
  property("test heap set state, generic methods") {
    implicit val key = "key"
    implicit val namespace = StateNamespaces.global
    implicit val stateId = "01"
    implicit val setValue1 = "setValue1"
    implicit val setValue2 = "setValue2"

    val map: Map[String, Table[String, String, Array[Byte]]]
    = new util.HashMap[String, Table[String, String, Array[Byte]]]()
    val factory = new HeapStateInternalsFactory[String](StringUtf8Coder.of, map)

    val stateInternals = factory.stateInternalsForKey(key)
    val setState = stateInternals.state[SetState[String]](namespace,
      StateTags.set(stateId, StringUtf8Coder.of))

    setState.add(setValue1)
    setState.add(setValue2)

    implicit var setStateIterator = setState.read.iterator()

    implicit var counter = 0
    while (setStateIterator.hasNext) {
      counter += 1
      setStateIterator.next()
    }

    counter shouldBe 2

    setState.addIfAbsent(setValue2).read shouldBe false

    setStateIterator = setState.read.iterator()

    counter = 0
    while (setStateIterator.hasNext) {
      counter += 1
      setStateIterator.next()
    }

    counter shouldBe 2

    setState.contains(setValue1).read shouldBe true
    setState.contains("setValue03").read shouldBe false

    setState.isEmpty.read shouldBe false

    setState.remove(setValue1)
    setState.remove(setValue2)

    setState.isEmpty.read shouldBe true
  }

  property("test heap set state, write state should not equal read state " +
    "with different key") {
    implicit val key = "key"
    implicit val newKey = "newKey"
    implicit val namespace = StateNamespaces.global
    implicit val stateId = "01"
    implicit val setValue1 = "setValue1"
    implicit val setValue2 = "setValue2"

    val map: Map[String, Table[String, String, Array[Byte]]]
    = new util.HashMap[String, Table[String, String, Array[Byte]]]()
    val factory = new HeapStateInternalsFactory[String](StringUtf8Coder.of, map)

    val stateInternals = factory.stateInternalsForKey(key)
    val newStateInternals = factory.stateInternalsForKey(newKey)

    val setState = stateInternals.state[SetState[String]](namespace,
      StateTags.set(stateId, StringUtf8Coder.of))
    val newSetState = newStateInternals.state(namespace,
      StateTags.set(stateId, StringUtf8Coder.of))

    setState.add(setValue1)
    setState.add(setValue2)

    implicit val newSetStateIterator = newSetState.read.iterator()

    var counter = 0
    while (newSetStateIterator.hasNext) {
      counter += 1
      newSetStateIterator.next()
    }

    counter shouldBe 0
  }

  property("test heap set state, write state shuold not equal read state " +
    "with different state id") {
    implicit val key = "key"
    implicit val newKey = "newKey"
    implicit val namespace = StateNamespaces.global
    implicit val stateId = "01"
    implicit val newStateId = "02"
    implicit val setValue1 = "setValue1"
    implicit val setValue2 = "setValue2"

    val map: Map[String, Table[String, String, Array[Byte]]]
    = new util.HashMap[String, Table[String, String, Array[Byte]]]()
    val factory = new HeapStateInternalsFactory[String](StringUtf8Coder.of, map)

    val stateInternals = factory.stateInternalsForKey(key)

    val setState = stateInternals.state(namespace, StateTags.set(stateId, StringUtf8Coder.of))
    val newSetState = stateInternals.state(namespace,
      StateTags.set(newStateId, StringUtf8Coder.of))

    setState.add(setValue1)
    setState.addIfAbsent(setValue2)

    implicit val setStateIterator = newSetState.read.iterator()

    var counter = 0
    while (setStateIterator.hasNext) {
      counter += 1
      setStateIterator.next()
    }

    counter shouldBe 0
  }
  // endregion

  // region map state
  property("test map state, generic methods") {
    implicit val key = "key"
    implicit val namespace = StateNamespaces.global
    implicit val stateId = "01"
    implicit val mapStateKey1 = "mapKey01"
    implicit val mapStateValue1 = "mapValue01"
    implicit val mapStateKey2 = "mapKey02"
    implicit val mapStateValue2 = "mapValue02"

    val map: Map[String, Table[String, String, Array[Byte]]]
    = new util.HashMap[String, Table[String, String, Array[Byte]]]()
    val factory = new HeapStateInternalsFactory[String](StringUtf8Coder.of, map)

    val stateInternals = factory.stateInternalsForKey(key)
    val mapState = stateInternals.state(namespace,
      StateTags.map(stateId, StringUtf8Coder.of, StringUtf8Coder.of))

    mapState.put(mapStateKey1, mapStateValue1)

    implicit var mapKeysIterator = mapState.keys.read.iterator()

    mapState.putIfAbsent(mapStateKey1, mapStateValue2).read shouldBe mapStateValue1

    var counter = 0
    while (mapKeysIterator.hasNext) {
      counter += 1
      mapKeysIterator.next()
    }

    counter shouldBe 1

    counter = 0
    implicit val mapValuesIterator = mapState.values.read.iterator()
    while (mapValuesIterator.hasNext) {
      counter += 1
      mapValuesIterator.next()
    }

    counter shouldBe 1

    counter = 0
    implicit val mapEntriesIterator = mapState.entries.read.iterator()
    while (mapEntriesIterator.hasNext) {
      counter += 1
      mapEntriesIterator.next()
    }

    counter shouldBe 1

    mapState.get(mapStateKey1).read shouldBe mapStateValue1
    mapState.get("test01").read shouldBe null

    mapState.remove(mapStateKey1)

    counter = 0
    mapKeysIterator = mapState.keys.read.iterator()
    while (mapKeysIterator.hasNext) {
      counter += 1
      mapKeysIterator.next()
    }

    counter shouldBe 0

    mapState.putIfAbsent(mapStateKey2, mapStateValue2)

    counter = 0
    mapKeysIterator = mapState.keys.read.iterator()
    while (mapKeysIterator.hasNext) {
      counter += 1
      mapKeysIterator.next()
    }

    counter shouldBe 1

    mapState.clear

    counter = 0
    mapKeysIterator = mapState.keys.read.iterator()
    while (mapKeysIterator.hasNext) {
      counter += 1
      mapKeysIterator.next()
    }

    counter shouldBe 0
  }

  property("test map state, write state should not equal read state " +
    "with different key") {
    implicit val key = "key"
    implicit val newKey = "newKey"
    implicit val namespace = StateNamespaces.global
    implicit val stateId = "01"
    implicit val mapStateKey1 = "mapKey01"
    implicit val mapStateValue1 = "mapValue01"
    implicit val mapStateKey2 = "mapKey02"
    implicit val mapStateValue2 = "mapValue02"

    val map: Map[String, Table[String, String, Array[Byte]]]
    = new util.HashMap[String, Table[String, String, Array[Byte]]]()
    val factory = new HeapStateInternalsFactory[String](StringUtf8Coder.of, map)

    val stateInternals = factory.stateInternalsForKey(key)
    val newStateInternals = factory.stateInternalsForKey(newKey)

    val mapState = stateInternals.state(namespace,
      StateTags.map(stateId, StringUtf8Coder.of, StringUtf8Coder.of))
    val newMapState = newStateInternals.state(namespace,
      StateTags.map(stateId, StringUtf8Coder.of, StringUtf8Coder.of))

    mapState.put(mapStateKey1, mapStateValue1)

    mapState.get(mapStateKey1).read shouldBe mapStateValue1
    newMapState.get(mapStateKey1).read shouldNot be(mapStateValue1)
  }

  property("test map state, write state should not equal read state " +
    "with different state id") {
    implicit val key = "key"
    implicit val namespace = StateNamespaces.global
    implicit val stateId = "01"
    implicit val newStateId = "02"
    implicit val mapStateKey1 = "mapKey01"
    implicit val mapStateValue1 = "mapValue01"
    implicit val mapStateKey2 = "mapKey02"
    implicit val mapStateValue2 = "mapValue02"

    val map: Map[String, Table[String, String, Array[Byte]]]
    = new util.HashMap[String, Table[String, String, Array[Byte]]]()
    val factory = new HeapStateInternalsFactory[String](StringUtf8Coder.of, map)

    val stateInternals = factory.stateInternalsForKey(key)

    val mapState = stateInternals.state(namespace,
      StateTags.map(stateId, StringUtf8Coder.of, StringUtf8Coder.of))
    val newMapState = stateInternals.state(namespace,
      StateTags.map(newStateId, StringUtf8Coder.of, StringUtf8Coder.of))

    mapState.put(mapStateKey1, mapStateValue1)

    mapState.get(mapStateKey1).read shouldBe mapStateValue1
    newMapState.get(mapStateKey1).read shouldNot be(mapStateValue1)
  }
  // endregion

}
