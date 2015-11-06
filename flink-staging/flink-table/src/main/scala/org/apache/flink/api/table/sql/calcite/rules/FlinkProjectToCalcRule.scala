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

package org.apache.flink.api.table.sql.calcite.rules

import org.apache.calcite.plan.RelOptRule.{any, operand, convert => convertTrait}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rex.RexProgram
import org.apache.flink.api.table.sql.calcite.nodes.{FlinkCalc, FlinkProject}

class FlinkProjectToCalcRule private extends RelOptRule(operand(classOf[FlinkProject], any)) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val project = call.rel(0).asInstanceOf[FlinkProject]
    val input = project.getInput
    val traitSet = project.getTraitSet
    // build program
    val program = RexProgram.create(
      input.getRowType,
      project.getProjects,
      null,
      project.getRowType,
      project.getCluster.getRexBuilder)
    // convert to calc
    val calc = new FlinkCalc(
      input.getCluster,
      traitSet,
      input,
      program)
    call.transformTo(calc)
  }

}

object FlinkProjectToCalcRule {
  val INSTANCE = new FlinkProjectToCalcRule()
}
