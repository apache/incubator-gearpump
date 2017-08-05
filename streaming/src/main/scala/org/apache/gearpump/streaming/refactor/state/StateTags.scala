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
import org.apache.gearpump.streaming.refactor.state.StateTags.StateKind.StateKind
import org.apache.gearpump.streaming.refactor.state.api._

object StateTags {

  object StateKind extends Enumeration {
    type StateKind = Value
    val SYSTEM = Value("s")
    val USER = Value("u")
  }

  private trait SystemStateTag[StateT <: State] {
    def asKind(kind: StateKind): StateTag[StateT]
  }

  def tagForSpec[StateT <: State](id: String, spec: StateSpec[StateT]): StateTag[StateT] =
    new SimpleStateTag[StateT](new StructureId(id), spec)

  def value[T](id: String, valueCoder: Coder[T]): StateTag[ValueState[T]] =
    new SimpleStateTag[ValueState[T]](new StructureId(id), StateSpecs.value(valueCoder))

  def bag[T](id: String, elemCoder: Coder[T]): StateTag[BagState[T]] =
    new SimpleStateTag[BagState[T]](new StructureId(id), StateSpecs.bag(elemCoder))

  def set[T](id: String, elemCoder: Coder[T]): StateTag[SetState[T]] =
    new SimpleStateTag[SetState[T]](new StructureId(id), StateSpecs.set(elemCoder))

  def map[K, V](id: String, keyCoder: Coder[K], valueCoder: Coder[V]): StateTag[MapState[K, V]] =
    new SimpleStateTag[MapState[K, V]](new StructureId(id), StateSpecs.map(keyCoder, valueCoder))

  private class SimpleStateTag[StateT <: State](id: StructureId, spec: StateSpec[StateT])
    extends StateTag[StateT] with SystemStateTag[StateT] {

    val aSpec: StateSpec[StateT] = spec
    val aId: StructureId = id

    override def appendTo(sb: Appendable): Unit = aId.appendTo(sb)


    override def getId: String = id.getRawId

    override def getSpec: StateSpec[StateT] = aSpec

    override def bind(binder: StateBinder): StateT = aSpec.bind(aId.getRawId, binder)

    override def asKind(kind: StateKind): StateTag[StateT] =
      new SimpleStateTag[StateT](aId.asKind(kind), aSpec)

    override def hashCode(): Int = Objects.hash(getClass, getId, getSpec)

    override def equals(obj: Any): Boolean = {
      if (!(obj.isInstanceOf[SimpleStateTag[_]])) false

      val otherTag: SimpleStateTag[_] = obj.asInstanceOf[SimpleStateTag[_]]
      Objects.equals(getId, otherTag.getId) && Objects.equals(getSpec, otherTag.getSpec)
    }
  }

  private class StructureId(kind: StateKind, rawId: String) extends Serializable {

    private val k: StateKind = kind
    private val r: String = rawId

    def this(rawId: String) {
      this(StateKind.USER, rawId)
    }

    def asKind(kind: StateKind): StructureId = new StructureId(kind, r)

    def appendTo(sb: Appendable): Unit = sb.append(k.toString).append(r)

    def getRawId: String = r

    override def hashCode(): Int = Objects.hash(k, r)

    override def equals(obj: Any): Boolean = {
      if (obj == this) true

      if (!(obj.isInstanceOf[StructureId])) false

      val that : StructureId = obj.asInstanceOf[StructureId]
      Objects.equals(k, that.k) && Objects.equals(r, that.r)
    }
  }

}
