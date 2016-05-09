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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.expression.utils.ExpressionTestBase
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.expressions.Literal
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.junit.Test

class DecimalTypeTest extends ExpressionTestBase {

  @Test
  def testDecimalLiterals(): Unit = {
    // implicit double
    testAllApis(
      11.2,
      "11.2",
      "11.2",
      "11.2")

    // implicit double
    testAllApis(
      0.7623533651719233,
      "0.7623533651719233",
      "0.7623533651719233",
      "0.7623533651719233")

    // explicit decimal (with precision of 19)
    testAllApis(
      BigDecimal("1234567891234567891"),
      "1234567891234567891p",
      "1234567891234567891",
      "1234567891234567891")

    // explicit decimal (high precision, not SQL compliant)
    testTableApi(
      BigDecimal("123456789123456789123456789"),
      "123456789123456789123456789p",
      "123456789123456789123456789")

    // explicit decimal (high precision, not SQL compliant)
    testTableApi(
      BigDecimal("12.3456789123456789123456789"),
      "12.3456789123456789123456789p",
      "12.3456789123456789123456789")
  }

  @Test
  def testDecimalBorders(): Unit = {
    testAllApis(
      Double.MaxValue,
      Double.MaxValue.toString,
      Double.MaxValue.toString,
      Double.MaxValue.toString)

    testAllApis(
      Double.MinValue,
      Double.MinValue.toString,
      Double.MinValue.toString,
      Double.MinValue.toString)

    testAllApis(
      Double.MinValue.cast(FLOAT_TYPE_INFO),
      s"${Double.MinValue}.cast(FLOAT)",
      s"CAST(${Double.MinValue} AS FLOAT)",
      Float.NegativeInfinity.toString)

    testAllApis(
      Byte.MinValue.cast(BYTE_TYPE_INFO),
      s"(${Byte.MinValue}).cast(BYTE)",
      s"CAST(${Byte.MinValue} AS TINYINT)",
      Byte.MinValue.toString)

    testAllApis(
      Byte.MinValue.cast(BYTE_TYPE_INFO) - 1.cast(BYTE_TYPE_INFO),
      s"(${Byte.MinValue}).cast(BYTE) - (1).cast(BYTE)",
      s"CAST(${Byte.MinValue} AS TINYINT) - CAST(1 AS TINYINT)",
      Byte.MaxValue.toString)

    testAllApis(
      Short.MinValue.cast(SHORT_TYPE_INFO),
      s"(${Short.MinValue}).cast(SHORT)",
      s"CAST(${Short.MinValue} AS SMALLINT)",
      Short.MinValue.toString)

    testAllApis(
      Int.MinValue.cast(INT_TYPE_INFO) - 1,
      s"(${Int.MinValue}).cast(INT) - 1",
      s"CAST(${Int.MinValue} AS INT) - 1",
      Int.MaxValue.toString)

    testAllApis(
      Long.MinValue.cast(LONG_TYPE_INFO),
      s"(${Long.MinValue}L).cast(LONG)",
      s"CAST(${Long.MinValue} AS BIGINT)",
      Long.MinValue.toString)
  }

  @Test
  def testDecimalCasting(): Unit = {
    // from String
    testTableApi(
      "123456789123456789123456789".cast(BIG_DEC_TYPE_INFO),
      "'123456789123456789123456789'.cast(DECIMAL)",
      "123456789123456789123456789")

    // from double
    testAllApis(
      'f3.cast(BIG_DEC_TYPE_INFO),
      "f3.cast(DECIMAL)",
      "CAST(f3 AS DECIMAL)",
      "4.2")

    // to double
    testAllApis(
      'f0.cast(DOUBLE_TYPE_INFO),
      "f0.cast(DOUBLE)",
      "CAST(f0 AS DOUBLE)",
      "1.2345678912345679E8")

    // to int
    testAllApis(
      'f4.cast(INT_TYPE_INFO),
      "f4.cast(INT)",
      "CAST(f4 AS INT)",
      "123456789")

    // to long
    testAllApis(
      'f4.cast(LONG_TYPE_INFO),
      "f4.cast(LONG)",
      "CAST(f4 AS BIGINT)",
      "123456789")

    // to boolean (not SQL compliant)
    testTableApi(
      'f1.cast(BOOLEAN_TYPE_INFO),
      "f1.cast(BOOL)",
      "true")

    testTableApi(
      'f5.cast(BOOLEAN_TYPE_INFO),
      "f5.cast(BOOL)",
      "false")

    testTableApi(
      BigDecimal("123456789.123456789123456789").cast(DOUBLE_TYPE_INFO),
      "(123456789.123456789123456789p).cast(DOUBLE)",
      "1.2345678912345679E8")
  }

  @Test
  def testDecimalArithmetic(): Unit = {
    // implicit cast to decimal
    testAllApis(
      'f1 + 12,
      "f1 + 12",
      "f1 + 12",
      "123456789123456789123456801")

    // implicit cast to decimal
    testAllApis(
      Literal(12) + 'f1,
      "12 + f1",
      "12 + f1",
      "123456789123456789123456801")

    // implicit cast to decimal
    testAllApis(
      'f1 + 12.3,
      "f1 + 12.3",
      "f1 + 12.3",
      "123456789123456789123456801.3")

    // implicit cast to decimal
    testAllApis(
      Literal(12.3) + 'f1,
      "12.3 + f1",
      "12.3 + f1",
      "123456789123456789123456801.3")

    testAllApis(
      'f1 + 'f1,
      "f1 + f1",
      "f1 + f1",
      "246913578246913578246913578")

    testAllApis(
      'f1 - 'f1,
      "f1 - f1",
      "f1 - f1",
      "0")

    testAllApis(
      'f1 * 'f1,
      "f1 * f1",
      "f1 * f1",
      "15241578780673678546105778281054720515622620750190521")

    testAllApis(
      'f1 / 'f1,
      "f1 / f1",
      "f1 / f1",
      "1")

    testAllApis(
      'f1 % 'f1,
      "f1 % f1",
      "MOD(f1, f1)",
      "0")

    testAllApis(
      -'f0,
      "-f0",
      "-f0",
      "-123456789.123456789123456789")
  }

  // ----------------------------------------------------------------------------------------------

  def testData = {
    val testData = new Row(6)
    testData.setField(0, BigDecimal("123456789.123456789123456789").bigDecimal)
    testData.setField(1, BigDecimal("123456789123456789123456789").bigDecimal)
    testData.setField(2, 42)
    testData.setField(3, 4.2)
    testData.setField(4, BigDecimal("123456789").bigDecimal)
    testData.setField(5, BigDecimal("0.000").bigDecimal)
    testData
  }

  def typeInfo = {
    new RowTypeInfo(Seq(
      BIG_DEC_TYPE_INFO,
      BIG_DEC_TYPE_INFO,
      INT_TYPE_INFO,
      DOUBLE_TYPE_INFO,
      BIG_DEC_TYPE_INFO,
      BIG_DEC_TYPE_INFO)).asInstanceOf[TypeInformation[Any]]
  }

}
