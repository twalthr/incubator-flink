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

package org.apache.flink.api.table.functions

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.{SqlFunction, SqlFunctionCategory, SqlIdentifier, SqlOperatorBinding}
import org.apache.flink.api.table.FlinkTypeFactory
import org.apache.flink.api.table.functions.GetFieldFunction.createReturnTypeInference

class GetFieldFunction
  extends SqlFunction(
    new SqlIdentifier("GET_FIELD", SqlParserPos.ZERO),
    createReturnTypeInference(),
    InferTypes.FIRST_KNOWN,
    OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER),
      OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER)),
    null,
    SqlFunctionCategory.SYSTEM) {

}

object GetFieldFunction {

  private def createReturnTypeInference(): SqlReturnTypeInference = {
    new SqlReturnTypeInference() {
      override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
        // check if first operator is composite
        val typeInfo = FlinkTypeFactory.toTypeInfo(opBinding.getOperandType(0))
        ???
      }
    }
  }

  val INSTANCE = new GetFieldFunction
}
