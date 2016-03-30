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

import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO
import org.apache.flink.api.table.codegen.calls.CallGenerator._
import org.apache.flink.api.table.codegen.{CodeGenerator, GeneratedExpression}

class ExtractCallGenerator() extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      logicalTypes: Seq[SqlTypeName],
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = {
    val method = BuiltInMethod.UNIX_DATE_EXTRACT.method
    // Date comes directly from SQL API e.g. via DATE '2003-01-01'
    // but is physically a timestamp.
    // It needs to be converted here since SQL has not converted it.
    if (logicalTypes(1) == SqlTypeName.DATE) {

    }
    generateCallIfArgsNotNull(codeGenerator.nullCheck, LONG_TYPE_INFO, operands) {
      (operandResultTerms) =>
        val timeUnitType = classOf[TimeUnitRange].getCanonicalName
        s"""
          |${method.getDeclaringClass.getCanonicalName}.
          |  ${method.getName}(
          |    $timeUnitType.values()[${operands.head.resultTerm}],
          |    ${operandResultTerms.drop(1).mkString(", ")});
         """.stripMargin
    }
  }
}
