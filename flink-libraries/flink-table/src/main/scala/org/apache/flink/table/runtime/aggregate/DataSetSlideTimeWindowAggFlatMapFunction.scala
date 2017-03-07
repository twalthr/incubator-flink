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
package org.apache.flink.table.runtime.aggregate

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}


/**
  * It is used for sliding windows on batch for time-windows. It takes a prepared input row,
  * aligns the window start, and replicates or omits records for different panes of a sliding
  * window. It is used for non-incremental aggregations.
  *
  * @param aggregates aggregate functions
  * @param groupingKeysLength number of grouping keys
  * @param windowSize window size of the sliding window
  * @param windowSlide window slide of the sliding window
  * @param returnType return type of this function
  */
class DataSetSlideTimeWindowAggFlatMapFunction(
    private val aggregates: Array[AggregateFunction[_ <: Any]],
    private val groupingKeysLength: Int,
    private val timeFieldPos: Int,
    private val windowSize: Long,
    private val windowSlide: Long,
    @transient private val returnType: TypeInformation[Row])
  extends RichFlatMapFunction[Row, Row]
  with ResultTypeQueryable[Row] {

  private var aggregateBuffer: Row = _
  private var outWindowStartIndex: Int = _

  override def open(config: Configuration) {
//    Preconditions.checkNotNull(aggregates)
//    // add one field to store window start
//    val partialRowLength = groupingKeysLength +
//      aggregates.map(_.intermediateDataType.length).sum + 1
//    aggregateBuffer = new Row(partialRowLength)
//    outWindowStartIndex = partialRowLength - 1
    ???
  }

  override def flatMap(record: Row, out: Collector[Row]): Unit = {
//    val windowStart = record.getField(timeFieldPos).asInstanceOf[Long]
//
//    // adopted from SlidingEventTimeWindows.assignWindows
//    var start: Long = TimeWindow.getWindowStartWithOffset(windowStart, 0, windowSlide)
//
//    // skip preparing output if it is not necessary
//    if (start > windowStart - windowSize) {
//
//      // prepare output
//      for (i <- aggregates.indices) {
//        aggregates(i).initiate(aggregateBuffer)
//        aggregates(i).merge(record, aggregateBuffer)
//      }
//      // set group keys value to final output
//      for (i <- 0 until groupingKeysLength) {
//        aggregateBuffer.setField(i, record.getField(i))
//      }
//
//      // adopted from SlidingEventTimeWindows.assignWindows
//      while (start > windowStart - windowSize) {
//        aggregateBuffer.setField(outWindowStartIndex, start)
//        out.collect(aggregateBuffer)
//        start -= windowSlide
//      }
//    }
    ???
  }

  override def getProducedType: TypeInformation[Row] = {
    returnType
  }
}
