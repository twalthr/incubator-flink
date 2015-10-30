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

import org.apache.calcite.plan.{RelOptCost, RelOptPlanner, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.util.{BitSets, ImmutableBitSet}
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.api.table.plan.{GroupBy, PlanNode, Select}
import org.apache.flink.api.table.sql.ExprUtils.toResField
import org.apache.flink.api.table.sql.PlanImplementor
import org.apache.flink.api.table.sql.calcite.{AggCallToExpr, FlinkRel}

import scala.collection.JavaConversions._

class FlinkAggregate(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    child: RelNode,
    indicator: Boolean,
    groupSet: ImmutableBitSet,
    groupSets: util.List[ImmutableBitSet],
    aggCalls: util.List[AggregateCall])
  extends Aggregate(
    cluster,
    traits,
    child,
    indicator,
    groupSet,
    groupSets,
    aggCalls)
  with FlinkRel {

  override def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      indicator: Boolean,
       groupSet: ImmutableBitSet,
      groupSets: util.List[ImmutableBitSet],
      aggCalls: util.List[AggregateCall]): Aggregate =
    new FlinkAggregate(
      getCluster,
      traitSet,
      input,
      indicator,
      groupSet,
      groupSets,
      aggCalls)


  override def computeSelfCost(planner: RelOptPlanner): RelOptCost = {
    // make the costs for some functions high such that the ReduceAggregatesRule
    // can convert them in sum and count aggregations
    val hasReduceableAggs = getAggCallList.exists(_.getAggregation.getName match {
        case "AVG" => true
        case "STDDEV_POP" => true
        case "STDDEV_SAMP" => true
        case "VAR_POP" => true
        case "VAR_SAMP" => true
        case _ => false
      })
    if (hasReduceableAggs) {
      planner.getCostFactory().makeHugeCost()
    }
    else {
      super.computeSelfCost(planner)
    }
  }

  override def translateToPlanNode(implementor: PlanImplementor): PlanNode = {
    val input = getInput.asInstanceOf[FlinkRel].translateToPlanNode(implementor)

    val groupFieldsIterable = for (fieldIdx <- BitSets.toIter(groupSet)) yield {
      toResField(input.outputFields(fieldIdx))
    }
    val groupFields = groupFieldsIterable.toSeq

    var allFields: Seq[Expression] = null
    if (groupFields.isEmpty) {
      allFields = input.outputFields.map(toResField(_))
    }

    // a row consists of grouping fields followed by agg fields
    val aggregateExprs = getRowType.getFieldNames.zipWithIndex.map(field => {
      val fieldIdx = field._2
      val fieldName = field._1

      // aggregate with no grouping
      if (groupFields.size == 0) {
        val aggCall = getAggCallList.apply(fieldIdx)
        AggCallToExpr.translate(aggCall, allFields, fieldName)
      }
      // aggregate with grouping
      else {
        // field reference
        if (fieldIdx < groupFields.size) {
          groupFields(fieldIdx)
        }
        // aggregate
        else {
          val aggCall = getAggCallList.apply(fieldIdx - groupFields.size)
          AggCallToExpr.translate(aggCall, groupFields, fieldName)
        }
      }
    })

    // translate with grouping
    if (!groupFields.isEmpty) {
      Select(GroupBy(input, groupFields), aggregateExprs)
    }
    // translate without grouping
    else {
      Select(input, aggregateExprs)
    }
  }
}
