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

import org.apache.calcite.plan.{RelTraitSet, RelOptCluster}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{AggregateCall, Aggregate}
import org.apache.calcite.util.{BitSets, ImmutableBitSet}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.expressions.ResolvedFieldReference
import org.apache.flink.api.table.plan.{Select, GroupBy, PlanNode}
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


  override def translateToPlanNode(implementor: PlanImplementor): PlanNode = {
    val input = getInput.asInstanceOf[FlinkRel].translateToPlanNode(implementor)

    val aggFieldsIterable = for (fieldIdx <- BitSets.toIter(groupSet)) yield {
      toResField(input.outputFields(fieldIdx))
    }
    var aggFields = aggFieldsIterable.toSeq

    // no aggregate fields -> use all input fields
    var grouping = true
    if (aggFields.isEmpty) {
      aggFields = input.outputFields.map(toResField(_)).toSeq
      grouping = false
    }

    val aggregateExprs = getRowType.getFieldNames.map(name => {
        // check for field aggregate
        val aggCall = getAggCallList.find(_.getName == name)
        if (aggCall.isDefined) {
          AggCallToExpr.translate(aggCall.get, aggFields)
        }
        else {
          aggFields.find(_.name == name).get
        }
    })

    // translate with grouping
    if (grouping) {
      Select(GroupBy(input, aggFields), aggregateExprs)
    }
    // translate without grouping
    else {
      Select(input, aggregateExprs)
    }
  }
}
