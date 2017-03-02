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

package org.apache.flink.table.calcite

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.validate.{SqlConformance, SqlValidatorImpl}
import org.apache.calcite.sql._

import scala.collection.JavaConverters._

/**
 * This is a copy of Calcite's CalciteSqlValidator to use with [[FlinkPlannerImpl]].
 */
class FlinkCalciteSqlValidator(
    opTab: SqlOperatorTable,
    catalogReader: CalciteCatalogReader,
    factory: JavaTypeFactory)
  extends SqlValidatorImpl(
    opTab,
    catalogReader,
    factory,
    SqlConformance.DEFAULT) {

  override def getLogicalSourceRowType(
      sourceRowType: RelDataType,
      insert: SqlInsert): RelDataType = {
    typeFactory.asInstanceOf[JavaTypeFactory].toSql(sourceRowType)
  }

  override def getLogicalTargetRowType(
      targetRowType: RelDataType,
      insert: SqlInsert): RelDataType = {
    typeFactory.asInstanceOf[JavaTypeFactory].toSql(targetRowType)
  }

//  override def expandStar(
//      selectList: SqlNodeList,
//      select: SqlSelect,
//      includeSystemVars: Boolean)
//    : SqlNodeList = {
//    val expandedList = super.expandStar(selectList, select, includeSystemVars)
//    val scope = getSelectScope(select)
//    val filteredList = expandedList.getList.asScala.filter { tpe =>
//      !FlinkTypeFactory.isProctimeIndicatorType(deriveType(scope, tpe))
//    }.asJava
//    getRawSelectScope(select).setExpandedSelectList(filteredList)
//    new SqlNodeList(filteredList, SqlParserPos.ZERO)
//  }
//
//  override def validateSelectList(
//      selectItems: SqlNodeList,
//      select: SqlSelect,
//      targetRowType: RelDataType)
//    : RelDataType = {
//    val rowType = super.validateSelectList(selectItems, select, targetRowType)
//    val filteredFieldList = rowType.getFieldList.asScala.filter { field =>
//      !FlinkTypeFactory.isProctimeIndicatorType(field.getType)
//    }
//    factory.createStructType(
//      filteredFieldList.map(_.getType).asJava,
//      filteredFieldList.map(_.getName).asJava)
//  }
}
