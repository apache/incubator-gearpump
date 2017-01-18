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
package org.apache.gearpump.cluster.client

import akka.pattern.ask
import akka.actor.ActorRef
import akka.util.Timeout
import org.apache.gearpump.cluster.ClientToMaster.{ResolveAppId, ShutdownApplication}
import org.apache.gearpump.cluster.MasterToClient.{ResolveAppIdResult, ShutdownApplicationResult}
import org.apache.gearpump.util.ActorUtil

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

class RunningApplication(val appId: Int, master: ActorRef, timeout: Timeout) {
  lazy val appMaster: ActorRef = resolveAppMaster(appId)

  def shutDown(): Unit = {
    val result = ActorUtil.askActor[ShutdownApplicationResult](master,
      ShutdownApplication(appId), timeout)
    result.appId match {
      case Success(_) =>
      case Failure(ex) => throw ex
    }
  }

  def askMaster[T](msg: Any): Future[T] = {
    appMaster.ask(msg)(timeout).asInstanceOf[Future[T]]
  }

  private def resolveAppMaster(appId: Int): ActorRef = {
    val result = ActorUtil.askActor[ResolveAppIdResult](master, ResolveAppId(appId), timeout)
    result.appMaster match {
      case Success(appMaster) => appMaster
      case Failure(ex) => throw ex
    }
  }
}

