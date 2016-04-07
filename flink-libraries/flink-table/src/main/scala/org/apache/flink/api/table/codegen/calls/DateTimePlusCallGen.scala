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

import org.apache.calcite.avatica.util.DateTimeUtils
import org.apache.calcite.rex.RexCall
import org.apache.calcite.sql.`type`.SqlTypeName.{DATE, TIME}
import org.apache.flink.api.table.codegen.calls.CallGenerator._
import org.apache.flink.api.table.codegen.{CodeGenerator, GeneratedExpression}

/**
  * Generates a function call for Date/Time arithmetic.
  */
class DateTimePlusCallGen extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      operands: Seq[GeneratedExpression],
      call: RexCall)
    : GeneratedExpression = {
    generateCallIfArgsNotNull(codeGenerator.nullCheck, operands.head.resultType, operands) {
      (operandResultTerms) =>
        val converted = getLogicalTypes(call).head match {
          case DATE =>
            s"((int) (${operandResultTerms(1)} / ${DateTimeUtils.MILLIS_PER_DAY}))"
          case TIME =>
            s"((int) ${operandResultTerms(1)})"
        }
        s"${operandResultTerms.head} + $converted"
    }
  }
}
