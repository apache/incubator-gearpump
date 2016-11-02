/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.akkastream.example

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.stream.scaladsl.{Broadcast, Merge, Sink, Source}
import org.apache.gearpump.akkastream.GearpumpMaterializer
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import org.apache.gearpump.util.AkkaApp

import scala.concurrent.Await
import scala.concurrent.duration._


/**
 * This is a simplified API you can use to combine sources and sinks
 * with junctions like: Broadcast[T], Balance[T], Merge[In] and Concat[A]
 * without the need for using the Graph DSL
 */

object Test7 extends AkkaApp with ArgumentsParser {
  // scalastyle:off println
  override val options: Array[(String, CLIOption[Any])] = Array(
    "gearpump" -> CLIOption[Boolean]("<boolean>", required = false, defaultValue = Some(false))
  )

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    implicit val system = ActorSystem("Test7", akkaConf)
    implicit val materializer: ActorMaterializer = config.getBoolean("gearpump") match {
      case true =>
        GearpumpMaterializer()
      case false =>
        ActorMaterializer(
          ActorMaterializerSettings(system).withAutoFusing(false)
        )
    }
    implicit val ec = system.dispatcher

    val sourceA = Source(List(1, 2, 3, 4, 5))
    val sourceB = Source(List(6, 7, 8, 9, 10))
    val mergedSource = Source.combine(sourceA, sourceB)(Merge(_))

    val sinkA = Sink.foreach[Int](x => println(s"In SinkA : $x"))
    val sinkB = Sink.foreach[Int](x => println(s"In SinkB : $x"))
    val sink = Sink.combine(sinkA, sinkB)(Broadcast[Int](_))
    mergedSource.runWith(sink)

    Await.result(system.whenTerminated, 60.minutes)
  }
  // scalastyle:on println
}
