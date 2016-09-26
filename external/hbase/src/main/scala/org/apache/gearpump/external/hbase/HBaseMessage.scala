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

import org.apache.hadoop.hbase.util.Bytes.toBytes

object HBaseMessage {

  case class DeleteMessage(
      rowKey: Array[Byte],
      columnFamily: Array[Byte],
      columns: List[Array[Byte]]) {

    def this(rowKey: String, columnFamily: String, columns: List[String]) =
      this(toBytes(rowKey), toBytes(columnFamily), columns.map(column => toBytes(column)))

    def this(rowKey: Array[Byte], columnFamily: Array[Byte], columnName: Array[Byte]) =
      this(rowKey, columnFamily, List(columnName))

    def this(rowKey: String, columnFamily: String, columnName: String) =
      this(toBytes(rowKey), toBytes(columnFamily), List(toBytes(columnName)))

  }


  case class IncrementMessage(
      rowKey: Array[Byte],
      columnFamily: Array[Byte],
      columnMap: Map[Array[Byte], Long]) {

    def this(rowKey: String, columnFamily: String, columnMap: Map[String, Long]) =
      this(toBytes(rowKey), toBytes(columnFamily),
        columnMap.map { case (key, value) => (toBytes(key), value) })

    def this(rowKey: Array[Byte], columnFamily: Array[Byte], columnName: Array[Byte], value: Long) =
      this(rowKey, columnFamily, Map(columnName, value)[Array[Byte], Long])

    def this(rowKey: String, columnFamily: String, columnName: String, value: Long) =
      this(toBytes(rowKey), toBytes(columnFamily),
        Map(toBytes(columnName), value)[Array[Byte], Long])

  }


  case class PutMessage(
      rowKey: Array[Byte],
      columnFamily: Array[Byte],
      columnMap: Map[Array[Byte], Array[Byte]]) {

    def this(rowKey: String, columnFamily: String, columnMap: Map[String, String]) =
      this(toBytes(rowKey), toBytes(columnFamily),
        columnMap.map { case (key, value) => (toBytes(key), toBytes(value)) })

    def this(rowKey: Array[Byte], columnFamily: Array[Byte], columnName: Array[Byte],
        value: Array[Byte]) =
      this(rowKey, columnFamily, Map(columnName, value)[Array[Byte], Array[Byte]])

    def this(rowKey: String, columnFamily: String, columnName: String, value: String) =
      this(toBytes(rowKey), toBytes(columnFamily),
        Map(toBytes(columnName), toBytes(value))[Array[Byte], Array[Byte]])

  }

}
