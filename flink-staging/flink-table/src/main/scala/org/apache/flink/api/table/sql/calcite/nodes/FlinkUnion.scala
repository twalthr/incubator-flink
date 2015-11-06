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
import org.apache.calcite.rel.core.Union
import org.apache.flink.api.table.plan.{As, GroupBy, PlanNode, UnionAll}
import org.apache.flink.api.table.sql.ExprUtils.toResFields
import org.apache.flink.api.table.sql.PlanImplementor
import org.apache.flink.api.table.sql.calcite.FlinkRel

import scala.collection.JavaConversions._

class FlinkUnion(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    inputs: util.List[RelNode],
    all: Boolean)
  extends Union(
    cluster,
    traits,
    inputs,
    all)
  with FlinkRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode], all: Boolean): Union =
    new FlinkUnion(
      getCluster,
      traitSet,
      inputs,
      all
    )

  override def translateToPlanNode(implementor: PlanImplementor): PlanNode = {
    val inputs = getInputs.map(_.asInstanceOf[FlinkRel].translateToPlanNode(implementor))
    val unionOfInputs = inputs.reduceLeft((left, right) => {
      // rename right side if field names are not equal
      val leftFieldNames = left.outputFields.map(_._1)
      val rightFieldNames = right.outputFields.map(_._1)
      if (!leftFieldNames.equals(rightFieldNames)) {
        UnionAll(left, As(right, leftFieldNames))
      }
      else {
        UnionAll(left, right)
      }
    })
    // do not group rows for UNION ALL
    if (all) {
      unionOfInputs
    }
    else {
      GroupBy(unionOfInputs, toResFields(inputs(0).outputFields))
    }
  }
}
