package org.apache.flink.api.table.sql.calcite

import java.util

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.api.table.plan.PlanNode
import org.apache.flink.api.table.sql.PlanImplementor

class FlinkTableScan(
    cluster: RelOptCluster,
    traits: RelTraitSet,
    table: RelOptTable,
    flinkTable: FlinkTable) extends TableScan(cluster, traits, table) with FlinkRel {

  override def register(planner: RelOptPlanner): Unit = {
    FlinkRules.CONVERTER_RULES.foreach(planner.addRule(_))
  }

  override def computeSelfCost(planner: RelOptPlanner): RelOptCost =
    super.computeSelfCost(planner).multiplyBy(1)

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = this

  override def translateToPlanNode(implementor: PlanImplementor): PlanNode = flinkTable.planNode
}
