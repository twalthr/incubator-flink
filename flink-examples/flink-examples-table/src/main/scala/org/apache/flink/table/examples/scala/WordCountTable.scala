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

package org.apache.flink.table.examples.scala

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.TimeIntervalUnit
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
  * Simple example for demonstrating the use of the Table API for a Word Count in Scala.
  *
  * This example shows how to:
  *  - Convert DataSets to Tables
  *  - Apply group, aggregate, select, and filter operations
  *
  */
object WordCountTable {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val input = env.fromElements(WC2("hello", 1, "a"), WC2("hello", 1, "b"), WC2("ciao", 1, "c"))
    val expr = input.toTable(tEnv, 'word, 'frequency.rowtime, 'w, 'proctime.proctime)
//     val result = expr.select('word, 'frequency).toDataStream[Row].print()
//     val result = expr.select('*).toDataStream[Row].print()
//     val result = expr.filter('proctime > 1L.toTimestamp).toDataStream[Row].print()
//
//    val result = expr.select('proctime as 'test).select('test).filter('test > 1L.toTimestamp)
//      .toDataStream[Row].print()
//
//    val result = expr.select('proctime.ceil(TimeIntervalUnit.DAY) as 'test).select('test)
//      .toDataStream[Row].print()
//
//    val expr2 = expr.select('word, 'w, 'proctime.ceil(TimeIntervalUnit.DAY),
//      'proctime.ceil(TimeIntervalUnit.DAY))
//    val result = expr.unionAll(expr2).toDataStream[Row].print()
//
//    val expr2 = expr.select('word, 'w, 'proctime.ceil(TimeIntervalUnit.DAY),
//      'proctime.ceil(TimeIntervalUnit.DAY))
//    val result = expr.unionAll(expr2).toDataStream[Row].print()
//
//    val tf = new MyTF
//    val result = expr.join(tf('proctime) as 'newWord).select('proctime, 'newWord)
//      .toDataStream[Row].print()
//
//    tEnv.registerTable("t", expr)
//    tEnv.sql("SELECT * FROM t").toDataStream[Row].print()
//
//    expr.window(Tumble over 4.millis on 'proctime as 'w).groupBy('w, 'word)
//      .select('w.start, 'word).toDataStream[Row].print()

    // expr.select('*).select('frequency.floor(TimeIntervalUnit.DAY)).toDataStream[Row].print()

    env.execute()
  }

  class MyTF extends TableFunction[String] {
    def eval(test: Long): String = {
      test.toString
    }
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class WC(word: String, frequency: Long)

  case class WC2(word: String, frequency: Long, w: String)

}
