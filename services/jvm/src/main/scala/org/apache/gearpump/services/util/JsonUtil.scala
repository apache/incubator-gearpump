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

package org.apache.gearpump.services.util

import org.apache.gearpump.cluster.{ApplicationStatus, UserConfig}
import org.apache.gearpump.cluster.worker.WorkerId
import org.apache.gearpump.metrics.Metrics.{Counter, Gauge, Histogram, Meter, MetricType, Timer}
import org.apache.gearpump.util.Graph
import org.json4s.{CustomSerializer, Formats, JValue, NoTypeHints, ShortTypeHints}
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}


object JsonUtil {

  // implicit val defaultFormats: Formats = Serialization.formats(NoTypeHints)
  implicit val defaultFormats: Formats = Serialization.formats(
    ShortTypeHints(List(classOf[Gauge], classOf[Histogram],
      classOf[Meter], classOf[Timer], classOf[Counter])))

  private object JSortedObject {
    def unapplySeq(value: JValue): Option[List[(String, JValue)]] = value match {
      case JObject(seq) =>
        val v = Some(seq.sortBy(_._1))
        v
      case _ => None
    }
  }

  implicit val graphSerializer: CustomSerializer[Graph[Int, String]] =
    new CustomSerializer[Graph[Int, String]](_ => ({
      case JSortedObject(
      ("edges", JString(e)),
      ("vertices", JString(v))
      ) =>
        val vertexList = read[List[Int]](v)
        val edgeList = read[List[(Int, String, Int)]](e)
        Graph(vertexList, edgeList)
    }, {
      case g: Graph[Int, String] =>
        ("vertices" -> JString(write(g.getVertices))) ~
          ("edges" -> JString(write(g.getEdges)))
    }))

  implicit val appStatusSerializer: CustomSerializer[ApplicationStatus] =
    new CustomSerializer[ApplicationStatus](_ => ({
      case JString(s) =>
        s match {
          case "pending" => ApplicationStatus.PENDING
          case "active" => ApplicationStatus.ACTIVE
          case "succeeded" => ApplicationStatus.SUCCEEDED
          case "failed" => ApplicationStatus.FAILED
          case "terminated" => ApplicationStatus.TERMINATED
        }
      case _ =>
        ApplicationStatus.NONEXIST
    }, {
      case as: ApplicationStatus =>
        JString(as.status)
    }))

  implicit val workerIdSerializer: CustomSerializer[WorkerId] =
    new CustomSerializer[WorkerId](_ => ({
      case JString(s) =>
        WorkerId(s)
      case _ =>
        WorkerId.unspecified
    }, {
      case id: WorkerId =>
        JString(id.toString)
    }))

  implicit val userConfigSerializer: CustomSerializer[UserConfig] =
    new CustomSerializer[UserConfig](_ => ({
      case JString(map) =>
        UserConfig(read[Map[String, String]](map))
    }, {
      case UserConfig(config) =>
        write(config)
    }))
}