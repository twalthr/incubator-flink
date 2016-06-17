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

package org.apache.flink.api.scala.expression

import java.sql.{Date, Time, Timestamp}

import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.api.scala.expression.utils.ExpressionTestBase
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.junit.Test

class TimeTypeTest extends ExpressionTestBase {

  @Test
  def testTimeLiterals(): Unit = {
    testAllApis(
      Date.valueOf("1990-10-14"),
      "'1990-10-14'.cast(DATE)",
      "DATE '1990-10-14'",
      "1990-10-14"
    )
  }

  // ----------------------------------------------------------------------------------------------

  def testData = {
    val testData = new Row(3)
    testData.setField(0, Date.valueOf("1990-10-14"))
    testData.setField(1, Time.valueOf("10:20:45"))
    testData.setField(2, Timestamp.valueOf("1990-10-14 10:20:45.123456789"))
    testData
  }

  def typeInfo = {
    new RowTypeInfo(Seq(
      SqlTimeTypeInfo.DATE,
      SqlTimeTypeInfo.TIME,
      SqlTimeTypeInfo.TIMESTAMP)).asInstanceOf[TypeInformation[Any]]
  }
}
