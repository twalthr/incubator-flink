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

import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

/**
  * It wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]].
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
class DataSetSlideWindowAggReduceGroupFunction(
    aggregates: Array[AggregateFunction[_ <: Any]],
    groupKeysMapping: Array[(Int, Int)],
    aggregateMapping: Array[(Int, Int)],
    finalRowArity: Int,
    finalRowWindowStartPos: Option[Int],
    finalRowWindowEndPos: Option[Int],
    windowSize: Long)
  extends RichGroupReduceFunction[Row, Row] {

  protected var aggregateBuffer: Row = _
  protected var windowStartFieldPos: Int = _

  private var collector: TimeWindowPropertyCollector = _
  private var output: Row = _

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupKeysMapping)
    aggregateBuffer = new Row(intermediateRowArity)
    windowStartFieldPos = intermediateRowArity - 1
    output = new Row(finalRowArity)
    collector = new TimeWindowPropertyCollector(finalRowWindowStartPos, finalRowWindowEndPos)
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    // initiate intermediate aggregate value.
    aggregates.foreach(_.initiate(aggregateBuffer))

    val iterator = records.iterator()
    while (iterator.hasNext) {
      val record = iterator.next()
      aggregates.foreach(_.merge(record, aggregateBuffer))

      // check if this record is the last record
      if (!iterator.hasNext) {
        // set group keys value to final output
        groupKeysMapping.foreach {
          case (after, previous) =>
            output.setField(after, record.getField(previous))
        }

        // evaluate final aggregate value and set to output
        aggregateMapping.foreach {
          case (after, previous) =>
            output.setField(after, aggregates(previous).evaluate(aggregateBuffer))
        }

        // adds TimeWindow properties to output then emit output
        if (finalRowWindowStartPos.isDefined || finalRowWindowEndPos.isDefined) {
          collector.wrappedCollector = out
          collector.windowStart = record.getField(windowStartFieldPos).asInstanceOf[Long]
          collector.windowEnd = collector.windowStart + windowSize

          collector.collect(output)
        } else {
          out.collect(output)
        }
      }
    }
  }
}
