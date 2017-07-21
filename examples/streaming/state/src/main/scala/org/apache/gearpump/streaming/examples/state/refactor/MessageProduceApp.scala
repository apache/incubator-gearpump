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

package org.apache.gearpump.streaming.examples.state.refactor

import java.util.Properties

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.streaming.hadoop.HadoopCheckpointStoreFactory
import org.apache.gearpump.streaming.hadoop.lib.rotation.FileSizeRotation
import org.apache.gearpump.streaming.kafka.{KafkaSink, KafkaSource}
import org.apache.gearpump.streaming.kafka.util.KafkaConfig
import org.apache.gearpump.streaming.partitioner.HashPartitioner
import org.apache.gearpump.streaming.sink.DataSinkProcessor
import org.apache.gearpump.streaming.source.DataSourceProcessor
import org.apache.gearpump.streaming.state.impl.PersistentStateConfig
import org.apache.gearpump.util.{AkkaApp, Graph, LogUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.gearpump.util.Graph.Node

/**
 *  a message produce app for producing message sequence to kafka
 */
object MessageProduceApp extends AkkaApp with ArgumentsParser {

  private val LOG = LogUtil.getLogger(getClass)

  val SEQUENCE_SINK_TOPIC = "sequenceSinkTopic"
  val SUM_RESULT_SINK_TOPIC = "sumResultSinkTopic"
  val ZOOKEEPER_CONNECT = "zookeeperConnect"
  val BROKER_LIST = "brokerList"
  val SOURCE_TOPIC = "sourceTopic"

  override val options: Array[(String, CLIOption[Any])] = Array(
    SOURCE_TOPIC -> CLIOption[String]("<kafka source topic>", required = true,
      defaultValue = Some("test")),
    SEQUENCE_SINK_TOPIC -> CLIOption[String]("<sequence kafka sink topic>", required = true),
    SUM_RESULT_SINK_TOPIC -> CLIOption[String]("<sum result kafka sink topic>", required = true),
    ZOOKEEPER_CONNECT -> CLIOption[String]("<Zookeeper connect string, e.g. localhost:2181/kafka>",
      required = true),
    BROKER_LIST -> CLIOption[String]("<Kafka broker list, e.g. localhost:9092>", required = true)
  )

  def application(config: ParseResult)(implicit system: ActorSystem): StreamApplication = {
    val appName = "MessageProduce"
    val hadoopConfig = new Configuration
    val checkpointStoreFactory = new HadoopCheckpointStoreFactory("MessageProduce", hadoopConfig,
      // Rotates on 1MB
      new FileSizeRotation(1000000))
    val taskConfig = UserConfig.empty
      .withBoolean(PersistentStateConfig.STATE_CHECKPOINT_ENABLE, true)
      .withLong(PersistentStateConfig.STATE_CHECKPOINT_INTERVAL_MS, 1000L)
      .withValue(PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY, checkpointStoreFactory)

    val properties = new Properties
    properties.put(KafkaConfig.ZOOKEEPER_CONNECT_CONFIG, config.getString(ZOOKEEPER_CONNECT))
    val brokerList = config.getString(BROKER_LIST)
    properties.put(KafkaConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    properties.put(KafkaConfig.CHECKPOINT_STORE_NAME_PREFIX_CONFIG, appName)


    val sequenceKafkaSink = new KafkaSink(config.getString(SEQUENCE_SINK_TOPIC), properties)
    val sequenceSinkProcessor = DataSinkProcessor(sequenceKafkaSink)
    val sourceTopic = config.getString(SOURCE_TOPIC)
    val kafkaSource = new KafkaSource(sourceTopic, properties)
    val sourceProcessor = DataSourceProcessor(kafkaSource)

    val sequencePartitioner = new HashPartitioner()

    val produceProcessor = Processor[ProduceProcessor](1, taskConf = taskConfig)

    val graph = Graph(sourceProcessor ~ sequencePartitioner ~> produceProcessor
      ~ sequencePartitioner ~> sequenceSinkProcessor)
    val app = StreamApplication(appName, graph, UserConfig.empty)
    app
  }

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val config = parse(args)
    val context = ClientContext(akkaConf)
    implicit val system = context.system
    val appId = context.submit(application(config))
    context.close()
  }
}
