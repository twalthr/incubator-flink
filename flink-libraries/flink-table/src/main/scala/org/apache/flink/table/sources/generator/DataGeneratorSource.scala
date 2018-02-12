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

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row

/**
  * Row generating data source.
  */
class DataGeneratorSource(
    val maxCount: Long,
    generators: Array[DataGenerator[_]],
    outputName: String,
    outputCode: String)
  extends RichParallelSourceFunction[Row]
  with Compiler[MapFunction[Row, Row]]
  with Logging {

  private var context: DataGeneratorContext = _
  private var data: Row = _
  private var function: MapFunction[Row, Row] = _

  @volatile
  private var isRunning: Boolean = true

  override def open(parameters: Configuration): Unit = {
    context = new DataGeneratorContext(maxCount, getRuntimeContext)
    data = new Row(generators.length)
    LOG.debug(s"Compiling MapFunction: $outputName \n\n Code:\n$outputCode")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, outputName, outputCode)
    LOG.debug("Instantiating MapFunction.")
    function = clazz.newInstance()
    FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext)
    FunctionUtils.openFunction(function, parameters)
  }

  override def run(ctx: SourceFunction.SourceContext[Row]): Unit = {
    while (isRunning && context.localCount < context.localMaxCount) {
      // generate data
      var i = 0
      while (i < generators.length) {
        data.setField(i, generators(i).generate(context))
        i += 1
      }
      // transform data
      ctx.getCheckpointLock.synchronized {
        ctx.collect(function.map(data))
      }
      context.localCount += 1
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
