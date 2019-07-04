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

package org.apache.flink.table.functions.utils

import java.util
import java.util.{List, Optional}

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.{SqlUserDefinedTableFunction, SqlUserDefinedTableMacro}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.{FunctionDefinition, TableFunction}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.plan.schema.FlinkTableFunctionImpl
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.inference.{CallContext, TypeStrategies}

/**
  * Calcite wrapper for user-defined table functions.
  */
class TableSqlFunction(
    name: String,
    displayName: String,
    tableFunction: TableFunction[_],
    rowTypeInfo: TypeInformation[_],
    typeFactory: FlinkTypeFactory,
    functionImpl: FlinkTableFunctionImpl[_])
  extends SqlUserDefinedTableFunction(
    new SqlIdentifier(name, SqlParserPos.ZERO),
    ReturnTypes.CURSOR,
    createEvalOperandTypeInference(name, tableFunction, typeFactory),
    createEvalOperandTypeChecker(name, tableFunction),
    null,
    functionImpl) {

  override def getRowType(
      typeFactory: RelDataTypeFactory,
      operandList: util.List[SqlNode])
    : RelDataType = {

    val inference = tableFunction.getTypeInference

    if (inference.getOutputTypeStrategy != TypeStrategies.MISSING) {
      val context = new TableSqlCallContext(name, tableFunction, typeFactory, operandList)
      inference.getOutputTypeStrategy.inferType(context)
    }
    super.getRowType(typeFactory, operandList)
  }

  /**
    * Get the user-defined table function.
    */
  def getTableFunction: TableFunction[_] = tableFunction

  /**
    * Get the type information of the table returned by the table function.
    */
  def getRowTypeInfo: TypeInformation[_] = rowTypeInfo

  /**
    * Get additional mapping information if the returned table type is a POJO
    * (POJO types have no deterministic field order).
    */
  def getPojoFieldMapping: Array[Int] = functionImpl.fieldIndexes

  override def isDeterministic: Boolean = tableFunction.isDeterministic

  override def toString: String = displayName

  class TableSqlCallContext(
      name: String,
      definition: FunctionDefinition,
      typeFactory: RelDataTypeFactory,
      operandList: util.List[SqlNode])
    extends CallContext {

    private val arguments = SqlUserDefinedTableMacro.convertArguments(
      typeFactory,
      operandList,
      function,
      getNameAsId,
      false)

    override def getArgumentDataTypes: util.List[DataType] = {

    }

    override def getFunctionDefinition: FunctionDefinition = ???

    override def isArgumentLiteral(pos: Int): Boolean = {

    }

    override def isArgumentNull(pos: Int): Boolean = {
      SqlUtil.isNullLiteral(operandList.get(pos), false)
    }

    override def getArgumentValue[T](pos: Int, clazz: Class[T]): Optional[T] = ???

    override def getName: String = ???
  }
}


