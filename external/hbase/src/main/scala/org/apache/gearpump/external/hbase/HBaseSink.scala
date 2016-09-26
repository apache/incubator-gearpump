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
package org.apache.gearpump.external.hbase

import java.io.{File, ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Delete, Increment, Put}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.security.{User, UserProvider}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.external.hbase.HBaseMessage.{DeleteMessage, IncrementMessage, PutMessage}
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.{Constants, FileUtils, LogUtil}
import org.slf4j.Logger

class HBaseSink(
    userconfig: UserConfig, tableName: String, @transient var configuration: Configuration)
  extends DataSink{
  val LOG: Logger = LogUtil.getLogger(getClass)
  lazy val connection = HBaseSink.getConnection(userconfig, configuration)
  lazy val table = connection.getTable(TableName.valueOf(tableName))

  override def open(context: TaskContext): Unit = {}

  def this(userconfig: UserConfig, tableName: String) = {
    this(userconfig, tableName, HBaseConfiguration.create())
  }

  def put(message: PutMessage): Unit = {
    val columnMap = message.columnMap
    val put = new Put(message.rowKey)
    columnMap.map {
      case (key, value) => put.addColumn(message.columnFamily, key, value)
    }
    table.put(put)
  }

  def delete(message: DeleteMessage): Unit = {
    val columns = message.columns
    val delete = new Delete(message.rowKey)
    columns.map(column => delete.addColumn(message.columnFamily, column))
    table.delete(delete)
  }

  def increment(message: IncrementMessage): Unit = {
    val columnMap = message.columnMap
    val increment = new Increment(message.rowKey)
    columnMap.map {
      case (key, value) => increment.addColumn(message.columnFamily, key, value)
    }
    table.increment(increment)
  }

  override def write(message: Message): Unit = {
    val msg = message.msg

    msg match {
      case putMessage: PutMessage => put(putMessage)
      case deleteMessage: DeleteMessage => delete(deleteMessage)
      case incrementMessage: IncrementMessage => increment(incrementMessage)
      case _ =>
    }
  }

  def close(): Unit = {
    table.close()
    connection.close()
  }

  /**
   * Overrides Java's default serialization
   * Please do not remove this
   */
  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    configuration.write(out)
  }

  /**
   * Overrides Java's default deserialization
   * Please do not remove this
   */
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    val clientConf = new Configuration(false)
    clientConf.readFields(in)
    configuration = HBaseConfiguration.create(clientConf)
  }
}

object HBaseSink {
  val HBASESINK = "hbasesink"
  val TABLE_NAME = "hbase.table.name"
  val COLUMN_FAMILY = "hbase.table.column.family"
  val COLUMN_NAME = "hbase.table.column.name"
  val HBASE_USER = "hbase.user"

  def apply[T](userconfig: UserConfig, tableName: String): HBaseSink = {
    new HBaseSink(userconfig, tableName)
  }

  def apply[T](userconfig: UserConfig, tableName: String, configuration: Configuration)
    : HBaseSink = {
    new HBaseSink(userconfig, tableName, configuration)
  }

  private def getConnection(userConfig: UserConfig, configuration: Configuration): Connection = {
    if (UserGroupInformation.isSecurityEnabled) {
      val principal = userConfig.getString(Constants.GEARPUMP_KERBEROS_PRINCIPAL)
      val keytabContent = userConfig.getBytes(Constants.GEARPUMP_KEYTAB_FILE)
      if (principal.isEmpty || keytabContent.isEmpty) {
        val errorMsg = s"HBase is security enabled, user should provide kerberos principal in " +
          s"${Constants.GEARPUMP_KERBEROS_PRINCIPAL} and keytab file " +
          s"in ${Constants.GEARPUMP_KEYTAB_FILE}"
        throw new Exception(errorMsg)
      }
      val keytabFile = File.createTempFile("login", ".keytab")
      FileUtils.writeByteArrayToFile(keytabFile, keytabContent.get)
      keytabFile.setExecutable(false)
      keytabFile.setWritable(false)
      keytabFile.setReadable(true, true)

      UserGroupInformation.setConfiguration(configuration)
      UserGroupInformation.loginUserFromKeytab(principal.get, keytabFile.getAbsolutePath)
      keytabFile.delete()
    }

    val userName = userConfig.getString(HBASE_USER)
    if (userName.isEmpty) {
      ConnectionFactory.createConnection(configuration)
    } else {
      val user = UserProvider.instantiate(configuration)
        .create(UserGroupInformation.createRemoteUser(userName.get))
      ConnectionFactory.createConnection(configuration, user)
    }

  }
}
