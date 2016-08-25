package org.apache.flink.api.table.plan.logical.rel

import java.util

import org.apache.calcite.plan.{Convention, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.core.{Aggregate, AggregateCall}
import org.apache.calcite.rel.{RelNode, RelShuttle}
import org.apache.calcite.util.ImmutableBitSet
import org.apache.flink.api.table.plan.logical.GroupWindow

class LogicalWindowAggregate(
    window: GroupWindow,
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    child: RelNode,
    indicator: Boolean,
    groupSet: ImmutableBitSet,
    groupSets: util.List[ImmutableBitSet],
    aggCalls: util.List[AggregateCall])
  extends Aggregate(
    cluster,
    traitSet,
    child,
    indicator,
    groupSet,
    groupSets,
    aggCalls) {

  def getWindow = window

  override def copy(
    traitSet: RelTraitSet,
    input: RelNode,
    indicator: Boolean,
    groupSet: ImmutableBitSet,
    groupSets: util.List[ImmutableBitSet],
    aggCalls: util.List[AggregateCall]): Aggregate = {
    new LogicalWindowAggregate(
      window,
      cluster,
      traitSet,
      input,
      indicator,
      groupSet,
      groupSets,
      aggCalls)
  }

  override def accept(shuttle: RelShuttle): RelNode = shuttle.visit(this)
}

object LogicalWindowAggregate {

  def create(window: GroupWindow, aggregate: Aggregate): LogicalWindowAggregate = {
    val cluster: RelOptCluster = aggregate.getCluster
    val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
    new LogicalWindowAggregate(
      window,
      cluster,
      traitSet,
      aggregate.getInput,
      aggregate.indicator,
      aggregate.getGroupSet,
      aggregate.getGroupSets,
      aggregate.getAggCallList)
  }
}
