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

package org.apache.flink.api.table.sql.calcite

import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelOptTable.ToRelContext
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.TranslatableTable
import org.apache.calcite.schema.impl.AbstractTable
import org.apache.flink.api.table.plan.PlanNode

class FlinkTable(val planNode: PlanNode) extends AbstractTable with TranslatableTable {
  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val builder = typeFactory.builder
    planNode.outputFields.foreach(field => {
      builder.add(field._1, typeFactory.createSqlType(TypeConverter.typeInfoToSqlType(field._2)))
    })
    builder.build
  }

  override def toRel(context: ToRelContext, relOptTable: RelOptTable): RelNode = {
    new FlinkTableScan(
      context.getCluster,
      context.getCluster.traitSetOf(FlinkRel.CONVENTION),
      relOptTable,
      this)
  }
}
