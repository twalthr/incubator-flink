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

import java.lang.reflect.Type
import java.util
import java.util.Collections

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.FunctionParameter
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`._
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.table.api.TableException
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.functions.utils.TableSqlFunction.{StatefulSqlReturnTypeInference, createEmptyCalciteTableFunction}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.table.functions.{FunctionDefinition, TableFunction}
import org.apache.flink.table.types.inference.TypeStrategies

/**
  * Calcite wrapper for user-defined table functions.
  */
class TableSqlFunction(
    name: String,
    displayName: String,
    tableFunction: TableFunction[_],
    rowTypeInfo: TypeInformation[_],
    fieldIndexes: Array[Int],
    fieldNames: Array[String],
    statefulSqlReturnTypeInference: StatefulSqlReturnTypeInference)
  extends SqlUserDefinedTableFunction(
    new SqlIdentifier(name, SqlParserPos.ZERO),
    ReturnTypes.CURSOR,
    createEvalOperandTypeInference(name, tableFunction),
    createEvalOperandTypeChecker(name, tableFunction),
    null,
    createEmptyCalciteTableFunction()) {

  if (fieldIndexes.length != fieldNames.length) {
    throw new TableException(
      "Number of field indexes and field names must be equal.")
  }

  // check uniqueness of field names
  if (fieldNames.length != fieldNames.toSet.size) {
    throw new TableException(
      "Table field names must be unique.")
  }

  override def getRowType(
      typeFactory: RelDataTypeFactory,
      operandList: util.List[SqlNode])
    : RelDataType = {

    val inference = tableFunction.getTypeInference

    if (inference.getOutputTypeStrategy != TypeStrategies.MISSING) {
      if (statefulSqlReturnTypeInference.outputType == null) {
        throw new IllegalStateException("Output type should not be null at this point.")
      }
      statefulSqlReturnTypeInference.outputType
    } else {
      legacyReturnType(typeFactory)
    }
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
  def getPojoFieldMapping: Array[Int] = fieldIndexes

  override def isDeterministic: Boolean = tableFunction.isDeterministic

  override def toString: String = displayName

  private def legacyReturnType(typeFactory: RelDataTypeFactory): RelDataType = {

    val fieldTypes: Array[TypeInformation[_]] = rowTypeInfo match {

      case ct: CompositeType[_] =>
        if (fieldNames.length != ct.getArity) {
          throw new TableException(
            s"Arity of type (" + ct.getFieldNames.deep + ") " +
              "not equal to number of field names " + fieldNames.deep + ".")
        }
        fieldIndexes.map(ct.getTypeAt(_).asInstanceOf[TypeInformation[_]])

      case t: TypeInformation[_] =>
        if (fieldIndexes.length != 1 || fieldIndexes(0) != 0) {
          throw new TableException(
            "Non-composite input type may have only a single field and its index must be 0.")
        }
        Array(t)
    }

    val flinkTypeFactory = typeFactory.asInstanceOf[FlinkTypeFactory]
    val builder = flinkTypeFactory.builder
    fieldNames
      .zip(fieldTypes)
      .foreach { f =>
        builder.add(f._1, flinkTypeFactory.createTypeFromTypeInfo(f._2, isNullable = true))
      }
    builder.build
  }
}

object TableSqlFunction {

  /**
    * This is a hack to make the (also hacky) Calcite implementation around table functions work.
    *
    * It calls the type inference at the correct stack location even though the result is used via
    * [[SqlUserDefinedTableFunction]]'s row type.
    */
  class StatefulSqlReturnTypeInference(
      name: String,
      definition: FunctionDefinition)
    extends SqlReturnTypeInference {

    var outputType: RelDataType = _

    override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
      val inference = definition.getTypeInference
      if (inference.getOutputTypeStrategy != TypeStrategies.MISSING) {
        outputType = new SqlReturnTypeInferenceBridge(name, definition).inferReturnType(opBinding)
      }
      ReturnTypes.CURSOR.inferReturnType(opBinding)
    }
  }

  def createEmptyCalciteTableFunction(): org.apache.calcite.schema.TableFunction = {
    new org.apache.calcite.schema.TableFunction {

      override def getRowType(
          typeFactory: RelDataTypeFactory,
          arguments: util.List[AnyRef])
        : RelDataType = {

        throw new UnsupportedOperationException("This should never be called.")
      }

      override def getElementType(arguments: util.List[AnyRef]): Type = classOf[Array[Object]]

      override def getParameters: util.List[FunctionParameter] = Collections.emptyList()
    }
  }
}


