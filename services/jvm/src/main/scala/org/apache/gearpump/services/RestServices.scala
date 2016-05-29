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

package org.apache.gearpump.services

import org.apache.gearpump.jarstore.local.LocalJarStoreService

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, _}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import org.apache.commons.lang.exception.ExceptionUtils

import org.apache.gearpump.jarstore.JarStoreService
import org.apache.gearpump.util.{Constants, LogUtil}
// NOTE: This cannot be removed!!!
import org.apache.gearpump.services.util.UpickleUtil._

/** Contains all REST API service endpoints */
class RestServices(master: ActorRef, mat: ActorMaterializer, system: ActorSystem)
  extends RouteService {

  private val LOG = LogUtil.getLogger(getClass)

  implicit val timeout = Constants.FUTURE_TIMEOUT

  private val config = system.settings.config

  // only LocalJarStoreService is supported now for "Compose DAG"
  // since DFSJarStoreService requires HDFS to be on the classpath.
  // Note this won't affect users  "Submit Gearpump Application" through
  // dashboard with "jarstore.rootpath" set to HDFS.
  if (!JarStoreService.get(config).isInstanceOf[LocalJarStoreService]) {
    LOG.warn("only local jar store is supported for Compose DAG")
  }
  private val jarStoreService = new LocalJarStoreService
  jarStoreService.init(config, system)


  private val securityEnabled = config.getBoolean(
    Constants.GEARPUMP_UI_SECURITY_AUTHENTICATION_ENABLED)

  private val supervisorPath = system.settings.config.getString(
    Constants.GEARPUMP_SERVICE_SUPERVISOR_PATH)

  private val myExceptionHandler: ExceptionHandler = ExceptionHandler {
    case ex: Throwable => {
      extractUri { uri =>
        LOG.error(s"Request to $uri could not be handled normally", ex)
        complete(InternalServerError, ExceptionUtils.getStackTrace(ex))
      }
    }
  }

  // Makes sure staticRoute is the final one, as it will try to lookup resource in local path
  // if there is no match in previous routes
  private val static = new StaticService(system, supervisorPath).route

  def supervisor: ActorRef = {
    if (supervisorPath == null || supervisorPath.isEmpty()) {
      null
    } else {
      val actorRef = system.actorSelection(supervisorPath).resolveOne()
      Await.result(actorRef, new Timeout(Duration.create(5, "seconds")).duration)
    }
  }

  override def route: Route = {
    if (securityEnabled) {
      val security = new SecurityService(services, system)
      handleExceptions(myExceptionHandler) {
        security.route ~ static
      }
    } else {
      handleExceptions(myExceptionHandler) {
        services.route ~ static
      }
    }
  }

  private def services: RouteService = {

    val admin = new AdminService(system)
    val masterService = new MasterService(master, jarStoreService, system)
    val worker = new WorkerService(master, system)
    val app = new AppMasterService(master, jarStoreService, system)
    val sup = new SupervisorService(master, supervisor, system)

    new RouteService {
      override def route: Route = {
        admin.route ~ sup.route ~ masterService.route ~ worker.route ~ app.route
      }
    }
  }
}