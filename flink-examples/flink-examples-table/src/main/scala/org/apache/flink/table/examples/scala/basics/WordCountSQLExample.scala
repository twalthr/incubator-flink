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

package org.apache.flink.table.examples.scala.basics

import org.apache.flink.table.api._

/** The famous word count example that shows a minimal Flink SQL job in batch execution mode. */
object WordCountSQLExample {

  def main(args: Array[String]): Unit = {

    // set up the Table API
    val settings = EnvironmentSettings.newInstance()
      .inBatchMode()
      .build()
    val tableEnv = TableEnvironment.create(settings)

    // execute a Flink SQL job and print the result locally
    tableEnv.fromValues(row(1, 1L, "Hello")).as("a", "b", "c").select('a, 'b, 'c)
      .where((1 to 30).map($"b" === _).reduce((ex1, ex2) => ex1 || ex2) && ($"c" === "xx")).execute().print()
  }
}
