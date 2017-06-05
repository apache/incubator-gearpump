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
package org.apache.gearpump.streaming.dsl.window.impl

import java.time.Instant

import com.gs.collections.api.block.predicate.Predicate
import com.gs.collections.api.block.procedure.Procedure
import com.gs.collections.impl.list.mutable.FastList
import com.gs.collections.impl.map.sorted.mutable.TreeSortedMap
import org.apache.gearpump.streaming.dsl.plan.functions.FunctionRunner
import org.apache.gearpump.streaming.dsl.window.api.WindowFunction.Context
import org.apache.gearpump.streaming.dsl.window.api.{Discarding, Windows}

import scala.collection.mutable.ArrayBuffer

trait WindowRunner[IN, OUT] extends java.io.Serializable {

  def process(in: IN, time: Instant): Unit

  def trigger(time: Instant): TraversableOnce[(OUT, Instant)]
}

case class AndThen[IN, MIDDLE, OUT](left: WindowRunner[IN, MIDDLE],
    right: WindowRunner[MIDDLE, OUT]) extends WindowRunner[IN, OUT] {

  def process(in: IN, time: Instant): Unit = {
    left.process(in, time)
  }

  def trigger(time: Instant): TraversableOnce[(OUT, Instant)] = {
    left.trigger(time).foreach(result => right.process(result._1, result._2))
    right.trigger(time)
  }
}

class DefaultWindowRunner[IN, OUT](
    windows: Windows,
    fnRunner: FunctionRunner[IN, OUT])
  extends WindowRunner[IN, OUT] {

  private val windowFn = windows.windowFn
  private val windowInputs = new TreeSortedMap[Window, FastList[(IN, Instant)]]
  private var setup = false

  override def process(in: IN, time: Instant): Unit = {
    val wins = windowFn(new Context[IN] {
      override def element: IN = in

      override def timestamp: Instant = time
    })
    wins.foreach { win =>
      if (windowFn.isNonMerging) {
        if (!windowInputs.containsKey(win)) {
          val inputs = new FastList[(IN, Instant)]
          windowInputs.put(win, inputs)
        }
        windowInputs.get(win).add(in -> time)
      } else {
        merge(windowInputs, win, in, time)
      }
    }

    def merge(
        winIns: TreeSortedMap[Window, FastList[(IN, Instant)]],
        win: Window, in: IN, time: Instant): Unit = {
      val intersected = winIns.keySet.select(new Predicate[Window] {
        override def accept(each: Window): Boolean = {
          win.intersects(each)
        }
      })
      var mergedWin = win
      val mergedInputs = FastList.newListWith(in -> time)
      intersected.forEach(new Procedure[Window] {
        override def value(each: Window): Unit = {
          mergedWin = mergedWin.span(each)
          mergedInputs.addAll(winIns.remove(each))
        }
      })
      winIns.put(mergedWin, mergedInputs)
    }
  }

  override def trigger(time: Instant): TraversableOnce[(OUT, Instant)] = {
    @annotation.tailrec
    def onTrigger(outputs: ArrayBuffer[(OUT, Instant)]): TraversableOnce[(OUT, Instant)] = {
      if (windowInputs.notEmpty()) {
        val firstWin = windowInputs.firstKey
        if (!time.isBefore(firstWin.endTime)) {
          val inputs = windowInputs.remove(firstWin)
          if (!setup) {
            fnRunner.setup()
            setup = true
          }
          inputs.forEach(new Procedure[(IN, Instant)] {
            override def value(v: (IN, Instant)): Unit = {
              fnRunner.process(v._1).foreach {
                out: OUT => outputs += (out -> v._2)
              }
            }
          })
          fnRunner.finish().foreach {
            out: OUT => outputs += (out -> firstWin.endTime.minusMillis(1))
          }
          if (windows.accumulationMode == Discarding) {
            fnRunner.teardown()
            setup = false
            // discarding, setup need to be called for each window
            onTrigger(outputs)
          } else {
            // accumulating, setup is only called for the first window
            onTrigger(outputs)
          }
        } else {
          outputs
        }
      } else {
        outputs
      }
    }

    onTrigger(ArrayBuffer.empty[(OUT, Instant)])
  }
}
