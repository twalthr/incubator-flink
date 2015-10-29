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
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.table.plan.{As, Filter, PlanNode, Join => JoinPlanNode}
import org.apache.flink.api.table.sql.PlanImplementor
import org.apache.flink.api.table.sql.calcite.{FlinkRel, RexToExpr}

class FlinkJoin(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    left: RelNode,
    right: RelNode,
    condition: RexNode,
    joinType: JoinRelType,
    variablesStopped: util.Set[String])
  extends Join(
    cluster,
    traits,
    left,
    right,
    condition,
    joinType,
    variablesStopped)
  with FlinkRel {

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join =
    new FlinkJoin(
      getCluster,
      traitSet,
      left,
      right,
      conditionExpr,
      joinType,
      getVariablesStopped
    )

  override def translateToPlanNode(implementor: PlanImplementor): PlanNode = {
    val left = getLeft.asInstanceOf[FlinkRel].translateToPlanNode(implementor)
    val right = getRight.asInstanceOf[FlinkRel].translateToPlanNode(implementor)

    getJoinType match {
      case JoinRelType.INNER => translateInnerJoin(left, right, implementor)
      case JoinRelType.LEFT => translateLeftJoin(left, right, implementor)
      case JoinRelType.RIGHT => translateRightJoin(left, right, implementor)
      case JoinRelType.FULL => translateFullJoin(left, right, implementor)
    }
  }

  def translateInnerJoin(left: PlanNode, right: PlanNode, implementor: PlanImplementor)
      : PlanNode = {
    // find field duplicates
    val outputFieldDuplicates = left.outputFields.intersect(right.outputFields)
    // rename field duplicates of left side
    if (outputFieldDuplicates.size > 0) {
      val newLeftOutputFields = left.outputFields.map(field =>
        if (outputFieldDuplicates.contains(field)) {
          ("tmp$" + field._1, field._2)
        }
        else {
          (field._1, field._2)
        }
      )
      val conditionExpr = RexToExpr.translate(condition, newLeftOutputFields, right.outputFields)
      As(
        Filter(
          JoinPlanNode(
            As(left, newLeftOutputFields.map(_._1)),
            right),
          conditionExpr),
        left.outputFields.map(_._1) ++ right.outputFields.map(_._1)
      )
    }
    // no rename
    else {
      val conditionExpr = RexToExpr.translate(condition, left.outputFields, right.outputFields)
      Filter(JoinPlanNode(left, right), conditionExpr)
    }
  }

  def translateLeftJoin(left: PlanNode, right: PlanNode, implementor: PlanImplementor): PlanNode = ???

  def translateRightJoin(left: PlanNode, right: PlanNode, implementor: PlanImplementor): PlanNode = ???

  def translateFullJoin(left: PlanNode, right: PlanNode, implementor: PlanImplementor): PlanNode = ???
}
