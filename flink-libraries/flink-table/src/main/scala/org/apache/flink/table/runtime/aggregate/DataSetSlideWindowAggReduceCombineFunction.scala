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

import org.apache.flink.api.common.functions.CombineFunction
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row

/**
  * Wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]] and
  * [[org.apache.flink.api.java.operators.GroupCombineOperator]].
  *
  * It is used for sliding on batch for both time and count-windows.
  *
  * @param aggregates aggregate functions.
  * @param groupKeysMapping index mapping of group keys between intermediate aggregate Row
  *                         and output Row.
  * @param aggregateMapping index mapping between aggregate function list and aggregated value
  *                         index in output Row.
  * @param finalRowArity output row field count
  * @param finalRowWindowStartPos relative window-start position to last field of output row
  * @param finalRowWindowEndPos relative window-end position to last field of output row
  * @param windowSize size of the window, used to determine window-end for output row
  */
class DataSetSlideWindowAggReduceCombineFunction(
    aggregates: Array[AggregateFunction[_ <: Any]],
    groupKeysMapping: Array[(Int, Int)],
    aggregateMapping: Array[(Int, Int)],
    finalRowArity: Int,
    finalRowWindowStartPos: Option[Int],
    finalRowWindowEndPos: Option[Int],
    windowSize: Long)
  extends DataSetSlideWindowAggReduceGroupFunction(
    aggregates,
    groupKeysMapping,
    aggregateMapping,
    finalRowArity,
    finalRowWindowStartPos,
    finalRowWindowEndPos,
    windowSize)
  with CombineFunction[Row, Row] {

  override def combine(records: Iterable[Row]): Row = {
    // reset first accumulator in merge list
    for (i <- aggregates.indices) {
      val accumulator = aggregates(i).createAccumulator()
      accumulatorList(i).set(0, accumulator)
    }

    val iterator = records.iterator()
    while (iterator.hasNext) {
      val record = iterator.next()

      for (i <- aggregates.indices) {
        // insert received accumulator into acc list
        val newAcc = record.getField(groupKeysMapping.length + i).asInstanceOf[Accumulator]
        accumulatorList(i).set(1, newAcc)
        // merge acc list
        val retAcc = aggregates(i).merge(accumulatorList(i))
        // insert result into acc list
        accumulatorList(i).set(0, retAcc)
      }

      // check if this record is the last record
      if (!iterator.hasNext) {
        // set group keys to aggregateBuffer
        for (i <- groupKeysMapping.indices) {
          aggregateBuffer.setField(i, record.getField(i))
        }

        // set the partial merged result to the aggregateBuffer
        for (i <- aggregates.indices) {
          aggregateBuffer.setField(groupKeysMapping.length + i, accumulatorList(i).get(0))
        }

        aggregateBuffer.setField(windowStartPos, record.getField(windowStartPos))

        return aggregateBuffer
      }
    }

    // this code path should never be reached as we return before the loop finishes
    throw new IllegalArgumentException("Group is empty. This should never happen.")
  }
}
