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
