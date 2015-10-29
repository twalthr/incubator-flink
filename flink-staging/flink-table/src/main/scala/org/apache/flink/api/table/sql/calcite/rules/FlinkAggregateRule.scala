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

import org.apache.calcite.plan.Convention
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.{LogicalJoin, LogicalAggregate}
import org.apache.flink.api.table.sql.calcite.FlinkRel
import org.apache.flink.api.table.sql.calcite.nodes.{FlinkAggregate, FlinkJoin}

import org.apache.calcite.plan.RelOptRule.{convert => convertTrait}

class FlinkAggregateRule private extends FlinkConverterRule(
  classOf[LogicalAggregate],
  Convention.NONE,
  "FlinkAggregateRule") {

  override def convert(rel: RelNode): RelNode = {
    val aggregate = rel.asInstanceOf[LogicalAggregate]
    new FlinkAggregate(aggregate.getCluster,
      aggregate.getTraitSet.replace(FlinkRel.CONVENTION),
      convertTrait(
        aggregate.getInput,
        aggregate.getInput.getTraitSet.replace(FlinkRel.CONVENTION)),
      aggregate.indicator,
      aggregate.getGroupSet,
      aggregate.getGroupSets,
      aggregate.getAggCallList)
  }
}

object FlinkAggregateRule {
  val INSTANCE = new FlinkAggregateRule()
}
