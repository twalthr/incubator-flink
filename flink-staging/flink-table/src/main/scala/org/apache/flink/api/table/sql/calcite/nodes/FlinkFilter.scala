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

import org.apache.calcite.plan.{RelTraitSet, RelOptCluster}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.table.plan.PlanNode
import org.apache.flink.api.table.sql.PlanImplementor
import org.apache.flink.api.table.sql.calcite.FlinkRel

class FlinkFilter(cluster: RelOptCluster, traitSet: RelTraitSet, child: RelNode,
    condition: RexNode) extends Filter(cluster, traitSet, child, condition) with FlinkRel {

  override def copy(traitSet: RelTraitSet, input: RelNode, condition: RexNode): Filter = {
    new FlinkFilter(getCluster, traitSet, input, condition)
  }

  override def translateToPlanNode(implementor: PlanImplementor): PlanNode = {
    null
  }

}
