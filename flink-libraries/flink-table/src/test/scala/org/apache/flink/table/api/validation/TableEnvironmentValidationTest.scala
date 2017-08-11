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

package org.apache.flink.table.api.validation

import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.types.Row
import org.junit.Assert.assertTrue
import org.junit._

class TableEnvironmentValidationTest {

  private val env = ExecutionEnvironment.getExecutionEnvironment

  @Test(expected = classOf[TableException])
  def testRegisterExistingDataSet(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val ds1 = CollectionDataSets.get3TupleDataSet(env)
    tEnv.registerDataSet("MyTable", ds1)
    val ds2 = CollectionDataSets.get5TupleDataSet(env)
    // Must fail. Name is already in use.
    tEnv.registerDataSet("MyTable", ds2)
  }

  @Test(expected = classOf[TableException])
  def testScanUnregisteredTable(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    // Must fail. No table registered under that name.
    tEnv.scan("someTable")
  }

  @Test(expected = classOf[TableException])
  def testRegisterExistingTable(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val t1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv)
    tEnv.registerTable("MyTable", t1)
    val t2 = CollectionDataSets.get5TupleDataSet(env).toTable(tEnv)
    // Must fail. Name is already in use.
    tEnv.registerDataSet("MyTable", t2)
  }

  @Test(expected = classOf[TableException])
  def testRegisterTableFromOtherEnv(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv1 = TableEnvironment.getTableEnvironment(env)
    val tEnv2 = TableEnvironment.getTableEnvironment(env)

    val t1 = CollectionDataSets.get3TupleDataSet(env).toTable(tEnv1)
    // Must fail. Table is bound to different TableEnvironment.
    tEnv2.registerTable("MyTable", t1)
  }

  @Test(expected = classOf[TableException])
  def testToTableWithToManyFields(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    CollectionDataSets.get3TupleDataSet(env)
      // Must fail. Number of fields does not match.
      .toTable(tEnv, 'a, 'b, 'c, 'd)
  }

  @Test(expected = classOf[TableException])
  def testToTableWithAmbiguousFields(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    CollectionDataSets.get3TupleDataSet(env)
      // Must fail. Field names not unique.
      .toTable(tEnv, 'a, 'b, 'b)
  }

  @Test(expected = classOf[TableException])
  def testToTableWithNonFieldReference1(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    // Must fail. as() can only have field references
    CollectionDataSets.get3TupleDataSet(env)
      .toTable(tEnv, 'a + 1, 'b, 'c)
  }

  @Test(expected = classOf[TableException])
  def testToTableWithNonFieldReference2(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    // Must fail. as() can only have field references
    CollectionDataSets.get3TupleDataSet(env)
      .toTable(tEnv, 'a as 'foo, 'b, 'c)
  }

  @Test(expected = classOf[TableException])
  def testGenericRow() {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    // use null value the enforce GenericType
    val dataSet = env.fromElements(Row.of(null))
    assertTrue(dataSet.getType().isInstanceOf[GenericTypeInfo[_]])
    assertTrue(dataSet.getType().getTypeClass == classOf[Row])

    // Must fail. Cannot import DataSet<Row> with GenericTypeInfo.
    tableEnv.fromDataSet(dataSet)
  }

  @Test(expected = classOf[TableException])
  def testGenericRowWithAlias() {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    // use null value the enforce GenericType
    val dataSet = env.fromElements(Row.of(null))
    assertTrue(dataSet.getType().isInstanceOf[GenericTypeInfo[_]])
    assertTrue(dataSet.getType().getTypeClass == classOf[Row])

    // Must fail. Cannot import DataSet<Row> with GenericTypeInfo.
    tableEnv.fromDataSet(dataSet, "nullField")
  }
}
