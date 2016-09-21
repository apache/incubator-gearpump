
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


package org.apache.gearpump.streaming.hbase

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.external.hbase.HBaseSink
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.hadoop.hbase.util.Bytes

class toHBase(hBaseSink: HBaseSink) extends DataSink {


  // val hbaseSink = new HBaseSink(userConfig, tableName)


  var x = 1
  while (x < 100) {
    hBaseSink.insert(Bytes.toBytes("row6"), Bytes.toBytes("group"),
      Bytes.toBytes("group:name"), Bytes.toBytes("100000"))
    x += 1
    // scalastyle:off
    println("This is : " + x )
  }

  override def open(context: TaskContext): Unit = {
    hBaseSink.open(context)
  }

  override def write(message: Message): Unit = {
   hBaseSink.write(message)
  }

  override def close(): Unit = {
    hBaseSink.close()
  }
}

