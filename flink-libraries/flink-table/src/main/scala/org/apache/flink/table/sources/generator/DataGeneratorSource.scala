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

package org.apache.flink.table.sources.generator

import java.util
import java.util.Collections

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.types.Row

class DataGeneratorSource(val maxCount: Long, fields: Array[GeneratedField])
  extends RichParallelSourceFunction[Row]
  with ListCheckpointed[Long] {

  private var subtaskCount:
  private var row: Row = _
  private var count: Long = 0L

  @volatile
  private var isRunning: Boolean = true

  override def open(parameters: Configuration): Unit = {
    row = new Row(fields.length)
  }

  override def run(ctx: SourceFunction.SourceContext[Row]): Unit = {
    while (isRunning && count < maxCount) {
      var i = 0
      while (i < fields.length) {
        row.setField(i, fields(i).next(count))
        i += 1
      }
      ctx.getCheckpointLock.synchronized {
        ctx.collect(row)
      }
      count += 1
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Long] = {
    Collections.singletonList(count)
  }

  override def restoreState(state: util.List[Long]): Unit = {

  }
}
