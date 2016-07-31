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
package org.apache.gearpump.experiments.cassandra.lib.connector.partitioner

import scala.collection.mutable.ArrayBuffer

/**
 * The original file (spark-cassandra-connector 1.6.0) was modified
 */
trait MonotonicBucketing[-T] {
  def bucket(n: Int): T => Int
}

object MonotonicBucketing {

  val lnOf2: Double = scala.math.log(2)
  def log2(x: Double): Double = scala.math.log(x) / lnOf2

  implicit object IntBucketing extends MonotonicBucketing[Int] {
    override def bucket(n: Int): Int => Int = {
      val shift = 31 - log2(n).toInt
      x => (x / 2 - Int.MinValue / 2) >> shift
    }
  }

  implicit object LongBucketing extends MonotonicBucketing[Long] {
    override def bucket(n: Int): Long => Int = {
      val shift = 63 - log2(n).toInt
      x => ((x / 2 - Long.MinValue / 2) >> shift).toInt
    }
  }
}

trait RangeBounds[-R, T] {
  def start(range: R): T
  def end(range: R): T
  def contains(range: R, point: T): Boolean
  def isFull(range: R): Boolean
}

class BucketingRangeIndex[R, T](
    ranges: Seq[R]
  )(implicit bounds: RangeBounds[R, T],
    ordering: Ordering[T], bucketing: MonotonicBucketing[T]) {

  private val sizeLog = MonotonicBucketing.log2(ranges.size).toInt + 1
  private val size = math.pow(2, sizeLog).toInt
  private val table = Array.fill(size)(new ArrayBuffer[R])
  private val bucket: T => Int = bucketing.bucket(size)

  private def add(r: R, startHash: Int, endHash: Int): Unit = {
    var i = startHash
    while (i <= endHash) {
      table(i) += r
      i += 1
    }
  }

  for (r <- ranges) {
    val start = bounds.start(r)
    val end = bounds.end(r)
    val startBucket = bucket(start)
    val endBucket = bucket(end)
    if (bounds.isFull(r)) {
      add(r, 0, size - 1)
    } else if (ordering.lt(end, start)) {
      add(r, startBucket, size - 1)
      add(r, 0, endBucket)
    }
    else {
      add(r, startBucket, endBucket)
    }
  }

  def rangesContaining(point: T): IndexedSeq[R] =
    table(bucket(point)).filter(bounds.contains(_, point))
}

