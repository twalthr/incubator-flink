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

import org.apache.flink.table.types.utils.ReflectiveDataTypeConverter

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

    val dt = ReflectiveDataTypeConverter.newInstance().build().extractDataType(classOf[WC2])

    println()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  // works
  case class WC0(w: java.util.Map[java.lang.Long, String])

  // does not work
  case class WC1(w: java.util.Map[Long, String])

  // does not work
  case class WC2(w: Either[String, String])

  // no tuples
  case class WC(word: String, bool: Boolean, frequency: (Long, Long), either: Either[Long, Boolean], option: Option[Long])

  class W {
    def x(l: Long, s: String): Unit = {

    }
  }

  class A[R] {
    var test: R = _


  }

  // LONG
  class T0 extends A[Long] {
  }

  class T1 extends A[java.lang.Long] {
  }

}
