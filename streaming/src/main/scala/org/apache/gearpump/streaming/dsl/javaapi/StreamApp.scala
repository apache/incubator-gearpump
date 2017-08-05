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

package org.apache.gearpump.streaming.dsl.javaapi

import com.typesafe.config.Config
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.{ClientContext, RunningApplication}
import org.apache.gearpump.streaming.dsl.scalaapi
import org.apache.gearpump.streaming.dsl.scalaapi.CollectionDataSource
import org.apache.gearpump.streaming.source.DataSource

import scala.collection.JavaConverters._

class StreamApp(name: String, context: ClientContext, userConfig: UserConfig) {

  def this(name: String, akkaConfig: Config) = {
    this(name, ClientContext.apply(akkaConfig), UserConfig.empty)
  }

  private val streamApp = scalaapi.StreamApp(name, context, userConfig)

  def source[T](collection: java.util.Collection[T], parallelism: Int,
      conf: UserConfig, description: String): Stream[T] = {
    val dataSource = new CollectionDataSource(collection.asScala.toSeq)
    source(dataSource, parallelism, conf, description)
  }

  def source[T](dataSource: DataSource, parallelism: Int,
      conf: UserConfig, description: String): Stream[T] = {
    new Stream[T](streamApp.source(dataSource, parallelism, conf, description))
  }

  def run(): RunningApplication = {
    streamApp.run()
  }
}
