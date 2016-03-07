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

import java.util.{Date, TimeZone}

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.test.utils.TableProgramsTestBase
import org.apache.flink.api.table.test.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class DateTimeITCase(
    mode: TestExecutionMode,
    config: TableConfigMode)
  extends TableProgramsTestBase(mode, config) {

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  @Test
  def testLiteral(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements((new Date(0), new Date(0))).as('date, 'date2)
      .select('date, new Date(1000))

    val expected = "Thu Jan 01 00:00:00 UTC 1970,Thu Jan 01 00:00:01 UTC 1970"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCastingFromDate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements((new Date(0), new Date(0))).as('date, 'date2)
      .select(
        'date.cast(STRING_TYPE_INFO),
        new Date(1000).cast(STRING_TYPE_INFO),
        new Date(22).cast(LONG_TYPE_INFO),
        new Date(42).cast(INT_TYPE_INFO),
        new Date(1).cast(BOOLEAN_TYPE_INFO))

    val expected = "1970-01-01 00:00:00,1970-01-01 00:00:01,22,42,true"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCastingToDate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val t = env.fromElements((new Date(0), new Date(0))).as('date, 'date2)
      .select(
        'date.cast(DATE_TYPE_INFO),
        "1970-01-02".cast(DATE_TYPE_INFO),
        "00:00:00".cast(DATE_TYPE_INFO),
        "1970-01-01 00:00:01".cast(DATE_TYPE_INFO),
        "1970-01-01 00:00:01.333".cast(DATE_TYPE_INFO))

    val expected = "1970-01-01 00:00:00,1970-01-01 00:00:01,22,42,true"
    val results = t.toDataSet[Row](getConfig).collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def test(): Unit = {
    ???
  }

}
