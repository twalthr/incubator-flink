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
import java.util.{ArrayList => JArrayList}

import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

/**
  * It is used for sliding windows on batch for time-windows. It takes a prepared input row (with
  * aligned rowtime for pre-tumbling), pre-aggregates (pre-tumbles) rows, aligns the window start,
  * and replicates or omits records for different panes of a sliding window.
  *
  * This function is similar to [[DataSetTumbleCountWindowAggReduceGroupFunction]], however,
  * it does no final aggregate evaluation. It also includes the logic of
  * [[DataSetSlideTimeWindowAggFlatMapFunction]].
  *
  * @param aggregates aggregate functions
  * @param groupingKeysLength number of grouping keys
  * @param timeFieldPos position of aligned time field
  * @param windowSize window size of the sliding window
  * @param windowSlide window slide of the sliding window
  * @param returnType return type of this function
  */
class DataSetSlideTimeWindowAggReduceGroupFunction(
    private val aggregates: Array[AggregateFunction[_ <: Any]],
    private val groupingKeysLength: Int,
    private val timeFieldPos: Int,
    private val windowSize: Long,
    private val windowSlide: Long,
    @transient private val returnType: TypeInformation[Row])
  extends RichGroupReduceFunction[Row, Row]
  with ResultTypeQueryable[Row] {

  protected var intermediateRow: Row = _
  protected val accumStartPos: Int = groupingKeysLength
  // add one field to store window start
  protected val intermediateRowArity: Int = accumStartPos + aggregates.length + 1

  val accumulatorList: Array[JArrayList[Accumulator]] = Array.fill(aggregates.length) {
    new JArrayList[Accumulator](2)
  }
  private val intermediateWindowStartPos: Int = intermediateRowArity - 1

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    intermediateRow = new Row(intermediateRowArity)

    // init lists with two empty accumulators
    for (i <- aggregates.indices) {
      val accumulator = aggregates(i).createAccumulator()
      accumulatorList(i).add(accumulator)
      accumulatorList(i).add(accumulator)
    }
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    // reset first accumulator
    for (i <- aggregates.indices) {
      val accumulator = aggregates(i).createAccumulator()
      accumulatorList(i).set(0, accumulator)
    }

    val iterator = records.iterator()

    while (iterator.hasNext) {
      val record = iterator.next()

      for (i <- aggregates.indices) {
        // insert received accumulator into acc list
        val newAcc = record.getField(accumStartPos + i).asInstanceOf[Accumulator]
        accumulatorList(i).set(1, newAcc)
        // merge acc list
        val retAcc = aggregates(i).merge(accumulatorList(i))
        // insert result into acc list
        accumulatorList(i).set(0, retAcc)
      }

      // trigger tumbling evaluation
      if (!iterator.hasNext) {
        val windowStart = record.getField(timeFieldPos).asInstanceOf[Long]

        // adopted from SlidingEventTimeWindows.assignWindows
        var start: Long = TimeWindow.getWindowStartWithOffset(windowStart, 0, windowSlide)

        // skip preparing output if it is not necessary
        if (start > windowStart - windowSize) {

          // set group keys value to buffer
          for (i <- 0 until groupingKeysLength) {
            intermediateRow.setField(i, record.getField(i))
          }

          // set accumulators
          for (i <- aggregates.indices) {
            intermediateRow.setField(accumStartPos + i, accumulatorList(i).get(0))
          }

          // adopted from SlidingEventTimeWindows.assignWindows
          while (start > windowStart - windowSize) {
            intermediateRow.setField(intermediateWindowStartPos, start)
            out.collect(intermediateRow)
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
