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

package org.apache.flink.api.table.sql

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools._
import org.apache.flink.api.table.Table
import org.apache.flink.api.table.sql.calcite.{FlinkRel, FlinkTable}

class SqlTranslator(val tableRegistry: TableRegistry) {

  private def schema(): SchemaPlus = {
    val schema = Frameworks.createRootSchema(true)
    tableRegistry.registry.foreach(table => {
      schema.add(table._1, new FlinkTable(table._2.operation))
    })
    schema
  }

  private def parserConfig(): SqlParser.Config = {
    SqlParser
      .configBuilder
      .setCaseSensitive(false)
      .build
  }

  private def traitSet(): RelTraitSet = {
    RelTraitSet.createEmpty.plus(FlinkRel.CONVENTION)
  }

  private def frameworkConfig(): FrameworkConfig = {
    Frameworks
      .newConfigBuilder
      .defaultSchema(schema)
      .parserConfig(parserConfig)
      .programs(Programs.standard)
      .traitDefs(FlinkRel.CONVENTION.getTraitDef)
      .build
  }

  def translate(sql: String): Table = {
    val planner = Frameworks.getPlanner(frameworkConfig)
    val parsed = planner.parse(sql)
    val validated = planner.validate(parsed)
    val converted = planner.convert(validated)
    val transformed = planner.transform(0, traitSet, converted)
    val implementor = new PlanImplementor(planner.getTypeFactory.asInstanceOf[JavaTypeFactory])
    val planNode = implementor.implement(transformed)
    Table(planNode)
  }

}
