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

package org.apache.flink.api.table.sql.calcite

import org.apache.calcite.rel.rules._
import org.apache.flink.api.table.sql.calcite.rules._

object FlinkRules {

  val CONVERTER_RULES = List(
    FlinkProjectRule.INSTANCE,
    FlinkFilterRule.INSTANCE,
    FlinkJoinRule.INSTANCE,
    FlinkUnionRule.INSTANCE,
    FlinkAggregateRule.INSTANCE,
    FlinkCalcRule.INSTANCE
    )

  // ALL RULES UNTIL FilterToCalcRule HAVE BEEN REVIEWED
  val RULES = List(
    // union rules
    UnionMergeRule.INSTANCE,
    UnionEliminatorRule.INSTANCE,
    // aggregate rules
    AggregateExpandDistinctAggregatesRule.INSTANCE,
    AggregateProjectMergeRule.INSTANCE,
    AggregateProjectPullUpConstantsRule.INSTANCE,
    AggregateReduceFunctionsRule.INSTANCE,
    AggregateRemoveRule.INSTANCE,
    AggregateUnionAggregateRule.INSTANCE,
    AggregateUnionTransposeRule.INSTANCE,
    // project rules
    FlinkProjectToCalcRule.INSTANCE,
    // filter rules
    FlinkFilterToCalcRule.INSTANCE,
    FilterSetOpTransposeRule.INSTANCE,
    // calc rules
    CalcMergeRule.INSTANCE,
    CalcRemoveRule.INSTANCE
    )

}
