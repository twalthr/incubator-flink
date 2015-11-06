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
import org.apache.calcite.plan.RelOptRule.{convert => convertTrait}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalUnion
import org.apache.flink.api.table.sql.calcite.nodes.FlinkUnion

import scala.collection.JavaConversions._

class FlinkUnionRule private extends FlinkConverterRule(
  classOf[LogicalUnion],
  Convention.NONE,
  "FlinkUnionRule") {

  override def convert(rel: RelNode): RelNode = {
    val union = rel.asInstanceOf[LogicalUnion]
    val inputs = union.getInputs.map(convertTrait(_, out))
    new FlinkUnion(
      rel.getCluster,
      rel.getTraitSet.replace(out),
      inputs,
      union.all)
  }
}

object FlinkUnionRule {
  val INSTANCE = new FlinkUnionRule()
}
