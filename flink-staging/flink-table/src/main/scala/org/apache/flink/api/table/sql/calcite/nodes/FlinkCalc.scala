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

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.AbstractRelNode.sole
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.RexProgram
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.expressions.{Expression, Naming}
import org.apache.flink.api.table.plan.{As, Select, Filter, PlanNode}
import org.apache.flink.api.table.sql.PlanImplementor
import org.apache.flink.api.table.sql.calcite.{RexToExpr, FlinkRel}

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

  override def explainTerms(pw: RelWriter): RelWriter =
    program.explainCalc(super.explainTerms(pw))

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode =
    new FlinkCalc(
      getCluster,
      traitSet,
      sole(inputs),
      program)

  override def translateToPlanNode(implementor: PlanImplementor): PlanNode = {
    val input = getInput.asInstanceOf[FlinkRel].translateToPlanNode(implementor)

    // return input if program is trivial
    if (program.isTrivial) {
      input
    }

    var filterNode: Filter = null
    // translate condition
    if (program.getCondition != null) {
      val condition = program.expandLocalRef(program.getCondition())
      val conditionExpr = RexToExpr.translate(condition, input.outputFields)
      filterNode = Filter(input, conditionExpr)
    }

    // translate projects
    var projectExprs: Seq[Expression] = null
    var renaming: Seq[(String, TypeInformation[_])] = null
    if (!program.projectsOnlyIdentity) {
      // rename fields to ensure uniqueness
      renaming = input.outputFields.zipWithIndex.map(fieldWithIndex =>
        ("tmp$" + fieldWithIndex._2, fieldWithIndex._1._2))

      // translate to expressions
      projectExprs = program.getNamedProjects.map(namedProject => {
        val project = program.expandLocalRef(namedProject.getKey)
        val projectExpr = RexToExpr.translate(project, renaming)
        Naming(projectExpr, namedProject.getValue)
      })
    }

    // complete translation
    // without condition
    if (filterNode == null) {
      Select(As(input, renaming.map(_._1)), projectExprs)
    }
    // without projection
    else if (projectExprs == null) {
      filterNode
    }
    // with projection and condition
    else {
      Select(As(filterNode, renaming.map(_._1)), projectExprs)
    }
  }
}
