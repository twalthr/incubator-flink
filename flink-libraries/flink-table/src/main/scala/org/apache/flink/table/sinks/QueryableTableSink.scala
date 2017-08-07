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

package org.apache.flink.table.sinks

import java.lang

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.{StreamQueryConfig, ValidationException}
import org.apache.flink.table.runtime.aggregate.ProcessFunctionWithCleanupState
import org.apache.flink.util.Collector

class QueryableTableSink[T](
    name: String,
    resultType: TypeInformation[T],
    queryConfig: StreamQueryConfig)
  extends TableSinkBase[tuple.Tuple2[lang.Boolean, T]]
  with UpsertStreamTableSink[T] {

  private var keys: Array[String] = _

  override def setKeyFields(keys: Array[String]): Unit = {
    if (keys == null) {
      throw ValidationException("A QueryableTableSink needs at least one key field.")
    }
    this.keys = keys
  }

  override def setIsAppendOnly(isAppendOnly: lang.Boolean): Unit = {
    if (isAppendOnly) {
      throw ValidationException("A QueryableTableSink can not be used with append-only tables " +
        "as the table would grow infinitely")
    }
  }

  override protected def copy: TableSinkBase[tuple.Tuple2[lang.Boolean, T]] = new QueryableTableSink[T](name, resultType, queryConfig)

  override def getRecordType: TypeInformation[T] = resultType

  override def emitDataStream(dataStream: DataStream[tuple.Tuple2[lang.Boolean, T]]): Unit = {
    val keyIndices = keys.map(getFieldNames.indexOf(_))
    dataStream.keyBy(keyIndices: _*).process(new QueryableStateProcessFunction(name, getRecordType, queryConfig))
  }
}

class QueryableStateProcessFunction[T](
    private val name: String,
    private val valueType: TypeInformation[T],
    private val queryConfig: StreamQueryConfig)
  extends ProcessFunctionWithCleanupState[tuple.Tuple2[lang.Boolean, T], Unit](queryConfig) {

  private var valueState: ValueState[T] = _

  override def open(parameters: Configuration): Unit = {
    val stateDesc = new ValueStateDescriptor("value", valueType)
    stateDesc.setQueryable(name)
    valueState = getRuntimeContext.getState(stateDesc)

    initCleanupTimeState("QueryableStateCleanupTime")
  }

  override def processElement(
      value: tuple.Tuple2[lang.Boolean, T],
      ctx: ProcessFunction[tuple.Tuple2[lang.Boolean, T], Unit]#Context,
      out: Collector[Unit]): Unit = {

    val currentTime = ctx.timerService().currentProcessingTime()
    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, currentTime)

    // handle upsert message
    if (value.f0) {
      valueState.update(value.f1)
    } else {
      valueState.clear()
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: ProcessFunction[tuple.Tuple2[lang.Boolean, T], Unit]#OnTimerContext,
      out: Collector[Unit]): Unit = {

    if (needToCleanupState(timestamp)) {
      cleanupState(valueState)
    }
  }
}
