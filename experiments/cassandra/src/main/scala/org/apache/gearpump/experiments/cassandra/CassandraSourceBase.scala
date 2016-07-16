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
package org.apache.gearpump.experiments.cassandra

import com.datastax.driver.core.{Row, Session}
import org.apache.gearpump.experiments.cassandra.lib.RowExtractor.RowExtractor
import org.apache.gearpump.experiments.cassandra.lib._
import org.apache.gearpump.streaming.transaction.api.{CheckpointStoreFactory, TimeReplayableSource}

private[cassandra] abstract class CassandraSourceBase[T: RowExtractor](
    connectorConf: CassandraConnectorConf,
    conf: ReadConf)
  extends TimeReplayableSource
  with Logging {

  protected var iterator: Option[Iterator[Row]] = None

  protected var connector: CassandraConnector = _
  protected var session: Session = _
  protected val rowExtractor = implicitly[RowExtractor[T]]

  override def setCheckpointStore(factory: CheckpointStoreFactory): Unit = { }
  override def close(): Unit = connector.evictCache()
}
