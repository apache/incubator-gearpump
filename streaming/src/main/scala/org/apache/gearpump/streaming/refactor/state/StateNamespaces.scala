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

/**
 *  a utility for state namespace
 */
object StateNamespaces {

  def global: StateNamespace = {
    new GlobalNameSpace
  }

  private object NameSpace extends Enumeration {
    type NameSpace = Value
    val GLOBAL, WINDOW, WINDOW_AND_TRIGGER = Value
  }

  class GlobalNameSpace extends StateNamespace {

    private val GLOBAL_STRING: String = "/"

    override def stringKey: String = {
      GLOBAL_STRING
    }

    override def appendTo(sb: Appendable): Unit = {
      sb.append(GLOBAL_STRING)
    }

    override def getCacheKey: AnyRef = {
      GLOBAL_STRING
    }

    override def equals(obj: Any): Boolean = {
      obj == this || obj.isInstanceOf[GlobalNameSpace]
    }

    override def hashCode(): Int = {
      Objects.hash(NameSpace.GLOBAL)
    }
  }

  // TODO : implement WindowNamespace & WindowAndTriggerNamespace

}
