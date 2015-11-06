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

package org.apache.flink.api.table.sql.calcite.nodes

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelOptCost, RelOptPlanner, RelTraitSet}
import org.apache.calcite.rel.AbstractRelNode.sole
import org.apache.calcite.rel.{RelNode, SingleRel}
import org.apache.calcite.rex.RexProgram
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.expressions.{Expression, Naming}
import org.apache.flink.api.table.plan.{As, Filter, PlanNode, Select}
import org.apache.flink.api.table.sql.PlanImplementor
import org.apache.flink.api.table.sql.calcite.{FlinkRel, RexToExpr}

import scala.collection.JavaConversions._

class FlinkCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    program: RexProgram)
  extends SingleRel(
    cluster,
    traitSet,
    input)
  with FlinkRel {

  this.rowType = program.getOutputRowType

  override def computeSelfCost(planner: RelOptPlanner): RelOptCost = {
    if (program.getCondition != null) {
      super.computeSelfCost(planner).multiplyBy(0.1)
    }
    else {
      getInput.computeSelfCost(planner)
    }
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode =
    new FlinkCalc(
      getCluster,
      traitSet,
      sole(inputs),
      program)

  override def translateToPlanNode(implementor: PlanImplementor): PlanNode = {
    val input = getInput.asInstanceOf[FlinkRel].translateToPlanNode(implementor)

    var filterNode: Option[Filter] = None
    // translate condition
    if (program.getCondition != null) {
      val condition = program.expandLocalRef(program.getCondition())
      val conditionExpr = RexToExpr.translate(condition, input.outputFields)
      filterNode = Some(Filter(input, conditionExpr))
    }

    // translate projection
    var projectExprs: Option[Seq[Expression]] = None
    var renamedFields: Option[Seq[(String, TypeInformation[_])]] = None

    // only renaming no real projection
    if (program.projectsOnlyIdentity
        && !program.getInputRowType.equals(rowType)) {
      renamedFields = Some(
        input
          .outputFields
          .zipWithIndex
          .map(fieldWithIndex =>
            (rowType.getFieldNames.get(fieldWithIndex._2), fieldWithIndex._1._2))
      )
    }
    // real projection
    else if (!program.projectsOnlyIdentity) {
      // rename input fields to ensure uniqueness
      renamedFields = Some(
        input
          .outputFields
          .zipWithIndex
          .map(fieldWithIndex => ("tmp$" + fieldWithIndex._2, fieldWithIndex._1._2))
      )

      // translate to expressions
      projectExprs = Some(
        program
          .getNamedProjects
          .map(namedProject => {
            val project = program.expandLocalRef(namedProject.getKey)
            val projectExpr = RexToExpr.translate(project, renamedFields.get)
            Naming(projectExpr, namedProject.getValue)
          })
      )
    }

    // complete translation

    // empty calc
    if (filterNode.isEmpty && renamedFields.isEmpty && projectExprs.isEmpty) {
      input
    }
    // without condition but with renaming and projection
    else if (filterNode.isEmpty && renamedFields.isDefined && projectExprs.isDefined) {
      Select(As(input, renamedFields.get.map(_._1)), projectExprs.get)
    }
    // without condition but with renaming only
    else if (filterNode.isEmpty && renamedFields.isDefined && projectExprs.isEmpty) {
      As(input, renamedFields.get.map(_._1))
    }
    // with condition and without renaming/projection
    else if (filterNode.isDefined && renamedFields.isEmpty && projectExprs.isEmpty) {
      filterNode.get
    }
    // with condition, renaming and projection
    else {
      Select(As(filterNode.get, renamedFields.get.map(_._1)), projectExprs.get)
    }
  }
}
