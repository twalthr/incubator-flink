package org.apache.flink.api.table.sql

import org.apache.calcite.adapter.enumerable.EnumerableConvention
import org.apache.calcite.plan.{RelTrait, RelTraitDef, RelTraitSet}
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{RuleSets, RuleSet, FrameworkConfig, Frameworks}
import org.apache.flink.api.table.plan.PlanNode
import org.apache.flink.api.table.sql.adapter.{FlinkRel, FlinkRules}

class SqlTranslator(val tableRegistry: TableRegistry) {

  private def createSchema(): SchemaPlus = {
    val schema = Frameworks.createRootSchema(true)
    tableRegistry.registry.foreach(table => {
      schema.add(table._1, new SqlTable(table._2.operation))
    })
    schema
  }

  private def createParserConfig(): SqlParser.Config = {
    SqlParser
      .configBuilder()
      .setCaseSensitive(false)
      .build()
  }


  private def createRuleSet(): RuleSet = {
    RuleSets.ofList(FlinkRules.rules())
  }

  private def createTraitSet(): RelTraitSet = {
    RelTraitSet.createEmpty().plus(FlinkRel.CONVENTION)
  }

  private def createFrameworkConfig(): FrameworkConfig = {
    Frameworks
      .newConfigBuilder()
      .defaultSchema(createSchema())
      .parserConfig(createParserConfig())
      .ruleSets(createRuleSet())
      .traitDefs(FlinkRel.CONVENTION.getTraitDef, EnumerableConvention.INSTANCE.getTraitDef)
      .build()
  }

  def translate(sql: String): PlanNode = {
    val planner = Frameworks.getPlanner(createFrameworkConfig)
    val sqlNode = planner.parse(sql)
    planner.validate(sqlNode)
    val relNode = planner.convert(sqlNode)
    val transformedRelNode = planner.transform(0, createTraitSet(), relNode)
    System.out.print(relNode)
    null
  }

}
