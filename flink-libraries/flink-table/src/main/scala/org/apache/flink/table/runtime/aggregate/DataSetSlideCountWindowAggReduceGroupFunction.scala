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

import java.lang.Iterable
import java.sql.Timestamp

import org.apache.calcite.runtime.SqlFunctions
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

/**
  * It is used for sliding windows on batch for count-windows. It takes a prepared input row,
  * pre-aggregates (pre-tumbles) rows, aligns the window start, and replicates or omits records
  * for different panes of a sliding window.
  *
  * @param aggregates aggregate functions
  * @param groupingKeysLength number of grouping keys
  * @param preTumblingSize number of records to be aggregated (tumbled) before emission
  * @param windowSize window size of the sliding window
  * @param windowSlide window slide of the sliding window
  * @param returnType return type of this function
  */
class DataSetSlideCountWindowAggReduceGroupFunction(
    private val aggregates: Array[AggregateFunction[_ <: Any]],
    private val groupingKeysLength: Int,
    private val preTumblingSize: Long,
    private val windowSize: Long,
    private val windowSlide: Long,
    @transient private val returnType: TypeInformation[Row])
  extends RichGroupReduceFunction[Row, Row]
  with ResultTypeQueryable[Row] {

  private var output: Row = _
  private var outWindowStartIndex: Int = _

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    // add one field to store window start count
    val partialRowLength = groupingKeysLength + aggregates.length + 1
    output = new Row(partialRowLength)
    outWindowStartIndex = partialRowLength - 1
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {
    var count: Long = 0

    val iterator = records.iterator()

    while (iterator.hasNext) {
      val record = iterator.next()
      // reset aggregates after completed tumbling
      if (count % preTumblingSize == 0) {
        // initiate intermediate aggregate value.
        aggregates.foreach(_.initiate(output))
      }

      // merge intermediate aggregate value to buffer.
      aggregates.foreach(_.merge(record, output))

      count += 1

      // trigger tumbling evaluation
      if (count % preTumblingSize == 0) {
        val windowStart = count
        // adopted from SlidingEventTimeWindows
        var start: Long = TimeWindow.getWindowStartWithOffset(windowStart, 0, windowSlide)

        // skip preparing output if it is not necessary
        if (start > windowStart - windowSize && start >= windowSlide) {
          // set group keys value to final output
          for (i <- 0 until groupingKeysLength) {
            output.setField(i, record.getField(i))
          }

          // emit the output
          // adopted from SlidingEventTimeWindows.assignWindows
          // however, we only emit if the first slide is reached
          while (start > windowStart - windowSize && start >= windowSlide) {
            output.setField(outWindowStartIndex, start)
            out.collect(output)
            start -= windowSlide
          }
        }
      }
    }
  }

  override def getProducedType: TypeInformation[Row] = {
    returnType
  }
}
