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
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.flink.api.table.sql.calcite.FlinkRel
import org.apache.flink.api.table.sql.calcite.nodes.FlinkJoin

class FlinkJoinRule private extends FlinkConverterRule(
    classOf[LogicalJoin],
    Convention.NONE,
    "FlinkJoinRule") {

  override def convert(rel: RelNode): RelNode = {
    val join = rel.asInstanceOf[LogicalJoin]
    new FlinkJoin(rel.getCluster,
      rel.getTraitSet.replace(FlinkRel.CONVENTION),
      join.getLeft,
      join.getRight,
      join.getCondition,
      join.getJoinType,
      join.getVariablesStopped)
  }
}

object FlinkJoinRule {
  val INSTANCE = new FlinkJoinRule()
}
