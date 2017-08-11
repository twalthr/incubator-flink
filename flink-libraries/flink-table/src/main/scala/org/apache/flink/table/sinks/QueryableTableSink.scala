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

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.typeutils.{ResultTypeQueryable, RowTypeInfo}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.api.{StreamQueryConfig, TableConfig, ValidationException}
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.codegen.FunctionCodeGenerator
import org.apache.flink.table.runtime.aggregate.ProcessFunctionWithCleanupState
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

class QueryableTableSink[S](
    queryableName: String,
    stateType: TypeInformation[S],
    tableConfig: TableConfig,
    queryConfig: StreamQueryConfig)
  extends TableSinkBase[tuple.Tuple2[lang.Boolean, Row]]
  with UpsertStreamTableSink[Row] {

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

  override protected def copy: TableSinkBase[tuple.Tuple2[lang.Boolean, Row]] =
    new QueryableTableSink[S](queryableName, stateType, tableConfig, queryConfig)

  override def getRecordType: TypeInformation[Row] = new RowTypeInfo(getFieldTypes, getFieldNames)

  override def emitDataStream(dataStream: DataStream[tuple.Tuple2[lang.Boolean, Row]]): Unit = {
    val keyIndices = keys.map(getFieldNames.indexOf(_))
    val keyTypes = keyIndices.map(getFieldTypes(_))
    val keySelectorType = new RowTypeInfo(keyTypes, keys)

    val converter = FunctionCodeGenerator.generateRowConverter(
      tableConfig,
      getRecordType,
      stateType,
      getFieldTypes,
      getFieldNames,
      "QueryableStateConverter")

    val processFunction = new QueryableStateProcessFunction(
      queryableName,
      stateType,
      queryConfig,
      converter.name,
      converter.code)

    dataStream
      .keyBy(new RowKeySelector(keyIndices, keySelectorType))
      .process(processFunction)
  }
}

class RowKeySelector(
    private val keyIndices: Array[Int],
    @transient private val returnType: TypeInformation[Row])
  extends KeySelector[tuple.Tuple2[lang.Boolean, Row], Row]
  with ResultTypeQueryable[Row] {

  override def getKey(value: tuple.Tuple2[lang.Boolean, Row]): Row = {
    val keys = keyIndices

    val srcRow = value.f1

    val destRow = new Row(keys.length)
    var i = 0
    while (i < keys.length) {
      destRow.setField(i, srcRow.getField(keys(i)))
      i += 1
    }

    destRow
  }

  override def getProducedType: TypeInformation[Row] = returnType
}

class QueryableStateProcessFunction[S](
    private val queryableName: String,
    private val stateType: TypeInformation[S],
    private val queryConfig: StreamQueryConfig,
    private val converterName: String,
    private val converterCode: String)
  extends ProcessFunctionWithCleanupState[tuple.Tuple2[lang.Boolean, Row], Void](queryConfig)
  with Compiler[MapFunction[Row, S]] {

  val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  private var converter: MapFunction[Row, S] = _
  private var valueState: ValueState[S] = _

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling MapFunction: $queryableName \n\n Code:\n$converterCode")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, converterName, converterCode)
    LOG.debug("Instantiating MapFunction.")
    converter = clazz.newInstance()

    val stateDesc = new ValueStateDescriptor("value", stateType)
    stateDesc.setQueryable(queryableName)
    valueState = getRuntimeContext.getState(stateDesc)

    initCleanupTimeState("QueryableStateCleanupTime")
  }

  override def processElement(
      value: tuple.Tuple2[lang.Boolean, Row],
      ctx: ProcessFunction[tuple.Tuple2[lang.Boolean, Row], Void]#Context,
      out: Collector[Void]): Unit = {

    val currentTime = ctx.timerService().currentProcessingTime()
    // register state-cleanup timer
    registerProcessingCleanupTimer(ctx, currentTime)

    // handle upsert message
    if (value.f0) {
      val converted = converter.map(value.f1)
      valueState.update(converted)
    } else {
      valueState.clear()
    }
  }

  override def onTimer(
      timestamp: Long,
      ctx: ProcessFunction[tuple.Tuple2[lang.Boolean, Row], Void]#OnTimerContext,
      out: Collector[Void]): Unit = {

    if (needToCleanupState(timestamp)) {
      cleanupState(valueState)
    }
  }
}
