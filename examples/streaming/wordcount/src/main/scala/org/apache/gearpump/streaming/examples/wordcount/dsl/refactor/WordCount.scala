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

package org.apache.gearpump.streaming.examples.wordcount.dsl.refactor

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.gearpump.streaming.refactor.coder.{StringUtf8Coder, VarLongCoder}
import org.apache.gearpump.streaming.refactor.dsl.api.functions.MapWithStateFunction
import org.apache.gearpump.streaming.refactor.dsl.scalaapi.StreamApp
import org.apache.gearpump.streaming.refactor.dsl.scalaapi.functions.FlatMapWithStateFunction
import org.apache.gearpump.streaming.refactor.state.api.{StateInternals, ValueState}
import org.apache.gearpump.streaming.refactor.state.{RuntimeContext, StateNamespaces, StateTags}
import org.apache.hadoop.conf.Configuration
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.cluster.client.ClientContext
import org.apache.gearpump.cluster.main.{ArgumentsParser, CLIOption, ParseResult}
import org.apache.gearpump.streaming.hadoop.HadoopCheckpointStoreFactory
import org.apache.gearpump.streaming.hadoop.lib.rotation.FileSizeRotation
import org.apache.gearpump.streaming.state.impl.PersistentStateConfig
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.{AkkaApp, Graph}

/**
 *
 */
object WordCount extends AkkaApp with ArgumentsParser {

  override val options: Array[(String, CLIOption[Any])] = Array.empty

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val context = ClientContext(akkaConf)

    val hadoopConfig = new Configuration
    val checkpointStoreFactory = new HadoopCheckpointStoreFactory("MessageConsume", hadoopConfig,
      // Rotates on 1MB
      new FileSizeRotation(1000000))
    val taskConfig = UserConfig.empty
      .withBoolean(PersistentStateConfig.STATE_CHECKPOINT_ENABLE, true)
      .withLong(PersistentStateConfig.STATE_CHECKPOINT_INTERVAL_MS, 1000L)
      .withValue(PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY,
        checkpointStoreFactory)(context.system)

    val app = StreamApp("dsl", context, taskConfig)
    val data = "This is a good start, bingo!! bingo!!"
    app.source(data.lines.toList, 1, "source").
      // word => (word, count)
      flatMapWithState(new StatefulFlatMapFunction(), "a stateful flatmap udf").
      mapWithState(new StatefulMapFunction(), "").
      // (word, count1), (word, count2) => (word, count1 + count2)
      groupByKey().sum.log

    context.submit(app).waitUntilFinish()
    context.close()
  }


  private class StatefulFlatMapFunction
    extends FlatMapWithStateFunction[String, String] {

    private val logger: Log = LogFactory.getLog(getClass)

    private implicit val counterStateTag = "tag1"

    private var stateInternals: Option[StateInternals] = None
    private var counterState: Option[ValueState[java.lang.Long]] = None

    override def setup(runtimeContext: RuntimeContext): Unit = {
      logger.info("StatefulFlatMapFunction setup.")
      stateInternals = Some(runtimeContext.getStateInternals(StringUtf8Coder.of, "partitionedKey"))

      counterState = Some(
        stateInternals.get.state(
          StateNamespaces.global, StateTags.value(counterStateTag, VarLongCoder.of))
      )

      // init
      if (counterState.get.read == null) {
        counterState.get.write(0L)
      }
    }


    override def flatMap(t: String): TraversableOnce[String] = {
      val oldVal = counterState.get.read
      logger.info("old value in flatmap : " + oldVal)
      counterState.get.write(oldVal + 1)

      t.split("[\\s]+")
    }

    override def teardown(runtimeContext: RuntimeContext): Unit = {
      logger.info("StatefulFlatMapFunction teardown.")
    }

  }

  private class StatefulMapFunction
    extends MapWithStateFunction[String, (String, Int)] {

    private val logger: Log = LogFactory.getLog(getClass)

    private implicit val counterStateTag = "tag2"

    private var stateInternals: Option[StateInternals] = None
    private var counterState: Option[ValueState[java.lang.Long]] = None

    override def setup(runtimeContext: RuntimeContext): Unit = {
      logger.info("StatefulMapFunction setup.")
      stateInternals = Some(runtimeContext.getStateInternals(StringUtf8Coder.of, "partitionedKey"))

      counterState = Some(
        stateInternals.get.state(
          StateNamespaces.global, StateTags.value(counterStateTag, VarLongCoder.of))
      )

      // init
      if (counterState.get.read == null) {
        counterState.get.write(0L)
      }
    }

    override def map(t: String): (String, Int) = {
      val oldVal = counterState.get.read
      logger.info("old value in map : " + oldVal)
      counterState.get.write(oldVal + 1)

      (t, 1)
    }

    override def teardown(runtimeContext: RuntimeContext): Unit = {
      logger.info("StatefulMapFunction teardown.")
    }

  }

}
