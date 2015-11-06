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

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rex.RexProgramBuilder
import org.apache.flink.api.table.sql.calcite.nodes.{FlinkCalc, FlinkFilter}

class FlinkFilterToCalcRule private extends RelOptRule(operand(classOf[FlinkFilter], any)) {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val filter = call.rel(0).asInstanceOf[FlinkFilter]
    val rel = filter.getInput
    val traitSet = filter.getTraitSet
    val rexBuilder = filter.getCluster.getRexBuilder
    val inputRowType = rel.getRowType

    // build program
    val programBuilder = new RexProgramBuilder(inputRowType, rexBuilder)
    programBuilder.addIdentity
    programBuilder.addCondition(filter.getCondition)
    val program = programBuilder.getProgram
    // convert to calc
    val calc = new FlinkCalc(
      filter.getCluster,
      traitSet,
      filter.getInput,
      program)
    call.transformTo(calc)
  }

}

object FlinkFilterToCalcRule {
  val INSTANCE = new FlinkFilterToCalcRule()
}
