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

package org.apache.flink.api.scala.table.test

import org.apache.flink.api.table.expressions.Literal
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.test.util.{TestBaseUtils, MultipleProgramsTestBase}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._


@RunWith(classOf[Parameterized])
class FilterITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {

  @Test
  def testAllRejectingFilter(): Unit = {
    /*
     * Test all-rejecting filter.
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)

    val filterDs = ds.filter( Literal(false) )

//    val expected = "\n"
//    val results = filterDs.collect()
//    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testAllPassingFilter(): Unit = {
    /*
     * Test all-passing filter.
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)

//    val filterDs = ds.filter( Literal(true) )
//    val expected = "1,1,Hi\n" + "2,2,Hello\n" + "3,2,Hello world\n" + "4,3,Hello world, " +
//      "how are you?\n" + "5,3,I am fine.\n" + "6,3,Luke Skywalker\n" + "7,4," +
//      "Comment#1\n" + "8,4,Comment#2\n" + "9,4,Comment#3\n" + "10,4,Comment#4\n" + "11,5," +
//      "Comment#5\n" + "12,5,Comment#6\n" + "13,5,Comment#7\n" + "14,5,Comment#8\n" + "15,5," +
//      "Comment#9\n" + "16,6,Comment#10\n" + "17,6,Comment#11\n" + "18,6,Comment#12\n" + "19," +
//      "6,Comment#13\n" + "20,6,Comment#14\n" + "21,6,Comment#15\n"
//    val results = filterDs.collect()
//    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterOnStringTupleField(): Unit = {
    /*
     * Test filter on String tuple field.
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val filterDs = ds.filter( _._3.contains("world") )

//    val expected = "(3,2,Hello world)\n" + "(4,3,Hello world, how are you?)\n"
//    val results = filterDs.collect()
//    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testFilterOnIntegerTupleField(): Unit = {
    /*
     * Test filter on Integer tuple field.
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env).as('a, 'b, 'c)

    val filterDs = ds.filter( 'a % 2 === 0 )

//    val expected = "2,2,Hello\n" + "4,3,Hello world, how are you?\n" +
//      "6,3,Luke Skywalker\n" + "8,4," + "Comment#2\n" + "10,4,Comment#4\n" +
//      "12,5,Comment#6\n" + "14,5,Comment#8\n" + "16,6," +
//      "Comment#10\n" + "18,6,Comment#12\n" + "20,6,Comment#14\n"
//    val results = filterDs.collect()
//    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  // These two not yet done, but are planned

  @Ignore
  @Test
  def testFilterBasicType(): Unit = {
    /*
     * Test filter on basic type
     */

    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getStringDataSet(env)

    val filterDs = ds.filter( _.startsWith("H") )

//    val expected = "Hi\n" + "Hello\n" + "Hello world\n" + "Hello world, how are you?\n"
//    val results = filterDs.collect()
//    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Ignore
  @Test
  def testFilterOnCustomType(): Unit = {
    /*
     * Test filter on custom type
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.getCustomTypeDataSet(env)
    val filterDs = ds.filter( _.myString.contains("a") )

//    val expected = "3,3,Hello world, how are you?\n" + "3,4,I am fine.\n" + "3,5,Luke Skywalker\n"
//    val results = filterDs.collect()
//    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}
