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

package org.apache.flink.api.table.sql.adapter;

import java.util.List;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;

public class EnumerableToFlinkConverter
		extends ConverterImpl
		implements FlinkRel {
	protected EnumerableToFlinkConverter(RelOptCluster cluster,
										 RelTraitSet traits, RelNode input) {
		super(cluster, ConventionTraitDef.INSTANCE, traits, input);
	}

	@Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
		return new EnumerableToFlinkConverter(
				getCluster(), traitSet, sole(inputs));
	}

	@Override public RelOptCost computeSelfCost(RelOptPlanner planner) {
		return super.computeSelfCost(planner).multiplyBy(.01);
	}

	@Override
	public void implement(Implementor implementor) {

	}
}
