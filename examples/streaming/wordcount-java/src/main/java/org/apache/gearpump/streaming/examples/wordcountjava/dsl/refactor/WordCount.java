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

package org.apache.gearpump.streaming.examples.wordcountjava.dsl.refactor;

import com.typesafe.config.Config;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.gearpump.DefaultMessage;
import org.apache.gearpump.Message;
import org.apache.gearpump.cluster.ClusterConfig;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.streaming.dsl.api.functions.ReduceFunction;
import org.apache.gearpump.streaming.hadoop.HadoopCheckpointStoreFactory;
import org.apache.gearpump.streaming.hadoop.lib.rotation.FileSizeRotation;
import org.apache.gearpump.streaming.refactor.coder.StringUtf8Coder;
import org.apache.gearpump.streaming.refactor.coder.VarLongCoder;
import org.apache.gearpump.streaming.refactor.dsl.api.functions.MapWithStateFunction;
import org.apache.gearpump.streaming.refactor.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.dsl.javaapi.functions.GroupByFunction;
import org.apache.gearpump.streaming.refactor.dsl.javaapi.JavaStreamApp;
import org.apache.gearpump.streaming.refactor.dsl.javaapi.functions.FlatMapWithStateFunction;
import org.apache.gearpump.streaming.refactor.state.RuntimeContext;
import org.apache.gearpump.streaming.refactor.state.StateNamespaces;
import org.apache.gearpump.streaming.refactor.state.StateTags;
import org.apache.gearpump.streaming.refactor.state.api.StateInternals;
import org.apache.gearpump.streaming.refactor.state.api.ValueState;
import org.apache.gearpump.streaming.source.DataSource;
import org.apache.gearpump.streaming.state.impl.PersistentStateConfig;
import org.apache.gearpump.streaming.task.TaskContext;
import org.apache.hadoop.conf.Configuration;
import scala.Tuple2;

import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;

/** Java version of WordCount with high level DSL API */
public class WordCount {

  public static void main(String[] args) throws InterruptedException {
    main(ClusterConfig.defaultConfig(), args);
  }

  public static void main(Config akkaConf, String[] args) throws InterruptedException {
    ClientContext context = new ClientContext(akkaConf);

    Configuration hadoopConfig = new Configuration();
    HadoopCheckpointStoreFactory checkpointStoreFactory = new HadoopCheckpointStoreFactory(
            "MessageConsume", hadoopConfig,
            // Rotates on 1MB
            new FileSizeRotation(1000000)
    );
    UserConfig taskConfig = UserConfig.empty()
            .withBoolean(PersistentStateConfig.STATE_CHECKPOINT_ENABLE(), true)
            .withLong(PersistentStateConfig.STATE_CHECKPOINT_INTERVAL_MS(), 1000L)
            .withValue(PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY(),
                    checkpointStoreFactory,
                    context.system()
            );

    JavaStreamApp app = new JavaStreamApp("JavaDSL", context, taskConfig);

    JavaStream<String> sentence = app.source(new StringSource("This is a good start, bingo!! bingo!!"),
        1, UserConfig.empty(), "source");

    JavaStream<String> words = sentence.flatMapWithState(new StatefulSplitFunction(), "flatMap");

    JavaStream<Tuple2<String, Integer>> ones = words.mapWithState(new StatefulMapFunction(), "map");

    JavaStream<Tuple2<String, Integer>> groupedOnes = ones.groupBy(new TupleKey(), 1, "groupBy");

    JavaStream<Tuple2<String, Integer>> wordcount = groupedOnes.reduce(new Count(), "reduce");

    wordcount.log();

    app.submit().waitUntilFinish();
    context.close();
  }

  private static class StringSource implements DataSource {

    private final String str;

    StringSource(String str) {
      this.str = str;
    }

    @Override
    public void open(TaskContext context, Instant startTime) {
    }

    @Override
    public Message read() {
      return new DefaultMessage(str, Instant.now());
    }

    @Override
    public void close() {
    }

    @Override
    public Instant getWatermark() {
      return Instant.now();
    }
  }

  private static class StatefulSplitFunction extends FlatMapWithStateFunction<String, String> {

    private static final Log logger = LogFactory.getLog(StatefulSplitFunction.class);

    private String counterStateTag = "tag1";

    private StateInternals stateInternal;
    private ValueState<Long> counterState;

    @Override
    public void setup(RuntimeContext runtimeContext) {
      logger.info("StatefulSplitFunction setup.");
      stateInternal = runtimeContext.getStateInternals(StringUtf8Coder.of(), "partitionedKey");

      counterState = stateInternal.state(StateNamespaces.global(), StateTags.value(counterStateTag, VarLongCoder.of()));

      if (counterState.read() == null) {
        counterState.write(0L);
      }
    }

    @Override
    public Iterator<String> flatMap(String s) {
      long oldVal = counterState.read();
      logger.info("old value in flatMap : " + oldVal);
      counterState.write(oldVal + 1);

      return Arrays.asList(s.split("\\s+")).iterator();
    }

    @Override
    public void teardown(RuntimeContext runtimeContext) {
      logger.info("StatefulSplitFunction teardown.");
    }
  }

  private static class StatefulMapFunction extends MapWithStateFunction<String, Tuple2<String, Integer>> {

    private static final Log logger = LogFactory.getLog(StatefulMapFunction.class);

    private String counterStateTag = "tag2";

    private StateInternals stateInternal;
    private ValueState<Long> counterState;

    @Override
    public void setup(RuntimeContext runtimeContext) {
      logger.info("StatefulMapFunction setup.");
      stateInternal = runtimeContext.getStateInternals(StringUtf8Coder.of(), "partitionedKey");

      counterState = stateInternal.state(StateNamespaces.global(), StateTags.value(counterStateTag, VarLongCoder.of()));

      if (counterState.read() == null) {
        counterState.write(0L);
      }
    }

    @Override
    public Tuple2<String, Integer> map(String s) {
      long oldVal = counterState.read();
      logger.info("old value in map method : " + oldVal);
      counterState.write(oldVal + 1);

      return new Tuple2<>(s, 1);
    }

    @Override
    public void teardown(RuntimeContext runtimeContext) {
      logger.info("StatefulMapFunction teardown.");
    }
  }

  private static class Count extends ReduceFunction<Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
      return new Tuple2<>(t1._1(), t1._2() + t2._2());
    }
  }

  private static class TupleKey extends GroupByFunction<Tuple2<String, Integer>, String> {

    @Override
    public String groupBy(Tuple2<String, Integer> tuple) {
      return tuple._1();
    }
  }
}
