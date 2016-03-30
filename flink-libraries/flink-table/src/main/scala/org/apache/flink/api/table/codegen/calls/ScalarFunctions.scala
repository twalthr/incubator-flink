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

package org.apache.flink.api.table.codegen.calls

import java.lang.reflect.Method

import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}

import scala.collection.mutable

/**
  * Global registry of built-in advanced SQL scalar functions.
  */
object ScalarFunctions {

  private val sqlFunctions: mutable.Map[(SqlOperator, Seq[TypeInformation[_]]), CallGenerator] =
    mutable.Map()

  // ----------------------------------------------------------------------------------------------
  // String functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunctionMethod(
    SUBSTRING,
    Seq(STRING_TYPE_INFO, INT_TYPE_INFO, INT_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.SUBSTRING.method)

  addSqlFunctionMethod(
    SUBSTRING,
    Seq(STRING_TYPE_INFO, INT_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.SUBSTRING.method)

  addSqlFunction(
    TRIM,
    Seq(INT_TYPE_INFO, STRING_TYPE_INFO, STRING_TYPE_INFO),
    new TrimCallGenerator())

  addSqlFunctionMethod(
    CHAR_LENGTH,
    Seq(STRING_TYPE_INFO),
    INT_TYPE_INFO,
    BuiltInMethod.CHAR_LENGTH.method)

  addSqlFunctionMethod(
    CHARACTER_LENGTH,
    Seq(STRING_TYPE_INFO),
    INT_TYPE_INFO,
    BuiltInMethod.CHAR_LENGTH.method)

  addSqlFunctionMethod(
    UPPER,
    Seq(STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.UPPER.method)

  addSqlFunctionMethod(
    LOWER,
    Seq(STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.LOWER.method)

  addSqlFunctionMethod(
    INITCAP,
    Seq(STRING_TYPE_INFO),
    STRING_TYPE_INFO,
    BuiltInMethod.INITCAP.method)

  addSqlFunctionMethod(
    LIKE,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO),
    BOOLEAN_TYPE_INFO,
    BuiltInMethod.LIKE.method)

  addSqlFunctionNotMethod(
    NOT_LIKE,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO),
    BuiltInMethod.LIKE.method)

  addSqlFunctionMethod(
    SIMILAR_TO,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO),
    BOOLEAN_TYPE_INFO,
    BuiltInMethod.SIMILAR.method)

  addSqlFunctionNotMethod(
    NOT_SIMILAR_TO,
    Seq(STRING_TYPE_INFO, STRING_TYPE_INFO),
    BuiltInMethod.SIMILAR.method)

  // ----------------------------------------------------------------------------------------------
  // Arithmetic functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunctionMethod(
    LOG10,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.LOG10)

  addSqlFunctionMethod(
    LN,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.LN)

  addSqlFunctionMethod(
    EXP,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.EXP)

  addSqlFunctionMethod(
    POWER,
    Seq(DOUBLE_TYPE_INFO, DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.POWER)

  addSqlFunctionMethod(
    ABS,
    Seq(BYTE_TYPE_INFO),
    BYTE_TYPE_INFO,
    BuiltInMethods.ABS)

  addSqlFunctionMethod(
    ABS,
    Seq(SHORT_TYPE_INFO),
    SHORT_TYPE_INFO,
    BuiltInMethods.ABS)

  addSqlFunctionMethod(
    ABS,
    Seq(INT_TYPE_INFO),
    INT_TYPE_INFO,
    BuiltInMethods.ABS)

  addSqlFunctionMethod(
    ABS,
    Seq(LONG_TYPE_INFO),
    LONG_TYPE_INFO,
    BuiltInMethods.ABS)

  addSqlFunctionMethod(
    ABS,
    Seq(FLOAT_TYPE_INFO),
    FLOAT_TYPE_INFO,
    BuiltInMethods.ABS)

  addSqlFunctionMethod(
    ABS,
    Seq(DOUBLE_TYPE_INFO),
    DOUBLE_TYPE_INFO,
    BuiltInMethods.ABS)


  // ----------------------------------------------------------------------------------------------
  // Date/Time functions
  // ----------------------------------------------------------------------------------------------

  addSqlFunction(
    EXTRACT_DATE,
    Seq(INT_TYPE_INFO, DATE_TYPE_INFO),
    new ExtractCallGenerator())

  addSqlFunction(
    EXTRACT,
    Seq(INT_TYPE_INFO, DATE_TYPE_INFO),
    new ExtractCallGenerator())

  // ----------------------------------------------------------------------------------------------

  def getCallGenerator(
      call: SqlOperator,
      operandTypes: Seq[TypeInformation[_]])
    : Option[CallGenerator] = {

    sqlFunctions.get((call, operandTypes))
      .orElse(sqlFunctions.find(entry => entry._1._1 == call
        && entry._1._2.length == operandTypes.length
        && entry._1._2.zip(operandTypes).forall {
        case (x@DATE_TYPE_INFO, y: BasicTypeInfo[_]) =>
          y.shouldAutocastTo(LONG_TYPE_INFO) || LONG_TYPE_INFO == y
        case (x: BasicTypeInfo[_], y@DATE_TYPE_INFO) =>
          LONG_TYPE_INFO.shouldAutocastTo(x) || x == LONG_TYPE_INFO
        case (x: BasicTypeInfo[_], y: BasicTypeInfo[_]) => y.shouldAutocastTo(x) || x == y
        case _ => false
      }).map(_._2))

  }

  // ----------------------------------------------------------------------------------------------

  private def addSqlFunctionMethod(
      sqlOperator: SqlOperator,
      operandTypes: Seq[TypeInformation[_]],
      returnType: TypeInformation[_],
      method: Method)
    : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) = new MethodCallGenerator(returnType, method)
  }

  private def addSqlFunctionNotMethod(
      sqlOperator: SqlOperator,
      operandTypes: Seq[TypeInformation[_]],
      method: Method)
    : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) =
      new NotCallGenerator(new MethodCallGenerator(BOOLEAN_TYPE_INFO, method))
  }

  private def addSqlFunction(
      sqlOperator: SqlOperator,
      operandTypes: Seq[TypeInformation[_]],
      callGenerator: CallGenerator)
    : Unit = {
    sqlFunctions((sqlOperator, operandTypes)) = callGenerator
  }

}
