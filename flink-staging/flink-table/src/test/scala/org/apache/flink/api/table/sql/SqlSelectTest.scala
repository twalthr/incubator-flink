package org.apache.flink.api.table.sql

import org.junit.Test
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._

class SqlSelectTest {

  @Test
  def testSimpleSelect(): Unit = {
    val tableRegistry = new TableRegistry

    val env = ExecutionEnvironment.getExecutionEnvironment
    val table = env.fromElements((1, "A"), (2, "B"), (3, "C"), (4, "D")).as('field1, 'field2)
    tableRegistry.registerTable("mytable", table)

    val translator = new SqlTranslator(tableRegistry)

    val plan = translator.translate("SELECT field1 FROM mytable WHERE field2 = 'A'")

    System.out.print(plan)
  }

}
