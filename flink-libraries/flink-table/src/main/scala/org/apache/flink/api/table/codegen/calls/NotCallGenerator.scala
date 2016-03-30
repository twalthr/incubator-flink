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

import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.api.table.codegen.calls.ScalarOperators.generateNot
import org.apache.flink.api.table.codegen.{GeneratedExpression, CodeGenerator}

/**
  * Inverts the boolean value of a CallGenerator result.
  */
class NotCallGenerator(callGenerator: CallGenerator) extends CallGenerator {

  override def generate(
      codeGenerator: CodeGenerator,
      logicalTypes: Seq[SqlTypeName],
      operands: Seq[GeneratedExpression])
    : GeneratedExpression = {
    val expr = callGenerator.generate(codeGenerator, logicalTypes, operands)
    generateNot(codeGenerator.nullCheck, expr)
  }

}
