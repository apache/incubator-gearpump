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
package org.apache.gearpump.experiments.cassandra.lib

import java.util.concurrent.Semaphore

import scala.collection.concurrent.TrieMap
import scala.util.Try

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture, SettableFuture}
import org.apache.gearpump.experiments.cassandra.lib.AsyncExecutor.Handler

class AsyncExecutor[T, R](
    asyncAction: T => ListenableFuture[R],
    maxConcurrentTasks: Int,
    successHandler: Option[Handler[T]] = None,
    failureHandler: Option[Handler[T]]) {

  @volatile private var _successful = true

  private val semaphore = new Semaphore(maxConcurrentTasks)
  private val pendingFutures = new TrieMap[ListenableFuture[R], Boolean]

  def executeAsync(task: T): ListenableFuture[R] = {
    val submissionTimestamp = System.nanoTime()
    semaphore.acquire()

    val settable = SettableFuture.create[R]()
    pendingFutures.put(settable, true)

    val executionTimestamp = System.nanoTime()
    val future = asyncAction(task)

    Futures.addCallback(future, new FutureCallback[R] {
      def release() {
        semaphore.release()
        pendingFutures.remove(settable)
      }
      def onSuccess(result: R) {
        release()
        settable.set(result)
        successHandler.foreach(_(task, submissionTimestamp, executionTimestamp))
      }
      def onFailure(throwable: Throwable) {
        if (_successful) _successful = false
        release()
        settable.setException(throwable)
        failureHandler.foreach(_(task, submissionTimestamp, executionTimestamp))
      }
    })

    settable
  }

  def waitForCurrentlyExecutingTasks() {
    for ((future, _) <- pendingFutures.snapshot())
      Try(future.get())
  }

  def successful: Boolean = _successful

}

object AsyncExecutor {
  type Handler[T] = (T, Long, Long) => Unit
}
