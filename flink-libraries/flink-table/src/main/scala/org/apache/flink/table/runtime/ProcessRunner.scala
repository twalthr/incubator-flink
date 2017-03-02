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

package org.apache.flink.table.runtime

//class ProcessRunner[IN, OUT](
//    name: String,
//    code: String,
//    @transient returnType: TypeInformation[OUT])
//  extends RichProcessFunction[IN, OUT]
//  with ResultTypeQueryable[OUT]
//  with Compiler[ProcessFunction[IN, OUT]] {
//
//  val LOG = LoggerFactory.getLogger(this.getClass)
//
//  private var function: ProcessFunction[IN, OUT] = _
//
//  override def open(parameters: Configuration): Unit = {
//    LOG.debug(s"Compiling ProcessFunction: $name \n\n Code:\n$code")
//    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
//    LOG.debug("Instantiating Function.")
//    function = clazz.newInstance()
//  }
//
//  override def processElement(value: IN, ctx: Context, out: Collector[OUT]): Unit =
//    function.processElement(value, ctx, out)
//
//  override def onTimer(timestamp: Long, ctx: OnTimerContext, out: Collector[OUT]): Unit =
//    function.onTimer(timestamp, ctx, out)
//
//  override def getProducedType: TypeInformation[OUT] = returnType
//
//}
