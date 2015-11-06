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

package org.apache.flink.api.table.sql

import org.apache.calcite.runtime.SqlFunctions
import org.apache.flink.util.InstantiationUtil
import org.junit.Assert._
import org.junit.{Ignore, Test}

class SqlSelectTest {

  @Test
  def testSimpleSelect(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
    val actual = sqlTestUtil.translator.translate("""
        SELECT *
        FROM TABLE1""")

    assertEquals(expected, actual)
  }

  @Test
  def testProjection(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .as("tmp$0, tmp$1")
      .select("tmp$0 + 1 as NEWFIELD")
    val actual = sqlTestUtil.translator.translate("""
        SELECT FIELD1 + 1 AS NEWFIELD
        FROM TABLE1""")

    assertEquals(expected, actual)
  }

  @Test
  def testFilter(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .filter("FIELD2 = 'test'")
    val actual = sqlTestUtil.translator.translate("""
      SELECT *
      FROM TABLE1
      WHERE FIELD2 = 'test'""")

    assertEquals(expected, actual)
  }

  @Test
  def testEquiJoin(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .as("tmp$FIELD1, tmp$FIELD2")
      .join(sqlTestUtil.table2)
      .where("tmp$FIELD1 = FIELD1")
      .as("FIELD1, FIELD2, FIELD1, FIELD2")
    val actual = sqlTestUtil.translator.translate("""
      SELECT *
      FROM TABLE1
      JOIN TABLE2 ON TABLE1.FIELD1 = TABLE2.FIELD1""")

    assertEquals(expected, actual)
  }

  @Test
  def testEquiJoinAndProject(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .as("tmp$FIELD1, tmp$FIELD2")
      .join(sqlTestUtil.table2)
      .where("tmp$FIELD1 = FIELD1")
      .as("FIELD1, FIELD2, FIELD1, FIELD2")
      .as("tmp$0, tmp$1, tmp$2, tmp$3")
      .select("tmp$3 AS FIELD2")
    val actual = sqlTestUtil.translator.translate("""
      SELECT TABLE2.FIELD2
      FROM TABLE1
      JOIN TABLE2 ON TABLE1.FIELD1 = TABLE2.FIELD1""")

    assertEquals(expected, actual)
  }

  @Test
  def testAggregateCount(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .select("FIELD1.count AS VAL")
    val actual = sqlTestUtil.translator.translate("""
        SELECT COUNT(*) AS VAL
        FROM TABLE1""")

    assertEquals(expected, actual)
  }

  @Test
  def testAggregateCountMinMaxSumPerGroup(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .groupBy("FIELD1")
      .select("FIELD1, FIELD1.min AS VALMIN, FIELD1.max AS VALMAX, FIELD1.count AS VALCOUNT, " +
      "FIELD1.sum AS VALSUM")
    val actual = sqlTestUtil.translator.translate("""
      SELECT FIELD1, MIN(FIELD1) AS VALMIN, MAX(FIELD1) AS VALMAX, COUNT(FIELD1) AS VALCOUNT,
        SUM(FIELD1) AS VALSUM
      FROM TABLE1
      GROUP BY FIELD1""")

    assertEquals(expected, actual)
  }

  @Test
  def testAggregateAvg(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .groupBy("FIELD1")
      .select("FIELD1, FIELD1.sum AS $f1, FIELD1.count AS $f2")
      .as("tmp$0, tmp$1, tmp$2")
      .select("tmp$0 AS FIELD1, (tmp$1 / tmp$2).cast(INT) AS VALAVG")
      .as("tmp$0, tmp$1")
      .select("tmp$1 AS VALAVG")
    val actual = sqlTestUtil.translator.translate("""
      SELECT AVG(FIELD1) AS VALAVG
      FROM TABLE1
      GROUP BY FIELD1""")

    assertEquals(expected, actual)
  }

  @Test
  def testDistinct(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .groupBy("FIELD1")
      .select("FIELD1")
    val actual = sqlTestUtil.translator.translate("""
      SELECT DISTINCT FIELD1
      FROM TABLE1""")

    assertEquals(expected, actual)
  }

  @Test
  def testBasicOperatorsWithCasting(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .filter("FIELD1 > 100.0 || (FIELD1 < 1 && !(FIELD1 = 10.0)) " +
      "|| FIELD1 != 11.0 || FIELD1 >= 11 || (FIELD1 <= 11 && FIELD2.isNull) || FIELD2.isNotNull")
    val actual = sqlTestUtil.translator.translate("""
      SELECT *
      FROM TABLE1
      WHERE FIELD1 > 100.0 OR (FIELD1 < 1 AND NOT (FIELD1 = 10.0)) OR FIELD1 <> 11.0
        OR FIELD1 >= 11 OR (FIELD1 <= 11 AND FIELD2 IS NULL) OR FIELD2 IS NOT NULL""")

    assertEquals(expected, actual)
  }

  @Test
  def testUnionAllOperator(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .unionAll(sqlTestUtil.table2)
    val actual = sqlTestUtil.translator.translate("""
      SELECT *
      FROM TABLE1
      UNION ALL
      SELECT *
      FROM TABLE2""")

    assertEquals(expected, actual)
  }

  @Test
  def testUnionAllOperatorWithProject(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .as("tmp$0, tmp$1")
      .select("tmp$0 AS FIELD1")
      .unionAll(
        sqlTestUtil.table2
          .as("tmp$0, tmp$1")
          .select("tmp$0 AS FIELD1")
      )
    val actual = sqlTestUtil.translator.translate("""
      SELECT FIELD1
      FROM TABLE1
      UNION ALL
      SELECT FIELD1
      FROM TABLE2""")

    assertEquals(expected, actual)
  }

  @Test
  def testUnionOperator(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .unionAll(sqlTestUtil.table2)
      .groupBy("FIELD1, FIELD2")
    val actual = sqlTestUtil.translator.translate("""
      SELECT *
      FROM TABLE1
      UNION
      SELECT *
      FROM TABLE2""")

    assertEquals(expected, actual)
  }

  @Test
  def testUnionOperatorWithProject(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .as("A, B")
      .unionAll(sqlTestUtil.table2.as("A, B"))
      .groupBy("A, B")
    val actual = sqlTestUtil.translator.translate("""
      SELECT FIELD1 AS A, FIELD2 AS B
      FROM TABLE1
      UNION
      SELECT FIELD1 AS A, FIELD2 AS B
      FROM TABLE2""")

    assertEquals(expected, actual)
  }




  // ----------------------------------------------------------------------------------------------
  // TODO
  // ----------------------------------------------------------------------------------------------


  @Test
  @Ignore
  def testUnionAllOperatorWith3Tables(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .as("A, B")
      .unionAll(sqlTestUtil.table2)
      .unionAll(sqlTestUtil.table3)
    val actual = sqlTestUtil.translator.translate("""
      SELECT FIELD1 AS A, FIELD2 AS B
      FROM TABLE1
      UNION ALL
      SELECT *
      FROM TABLE2
      UNION ALL
      SELECT *
      FROM TABLE3""")

    assertEquals(expected, actual)
  }



  @Test
  @Ignore
  def testBetweenOperator(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .filter("FIELD1 >= 0 && FIELD1 <= 10 && FIELD2 >= 'A' && FIELD2 <= 'C'")
    val actual = sqlTestUtil.translator.translate("""
      SELECT *
      FROM TABLE1
      WHERE FIELD1 BETWEEN 0 AND 10 AND FIELD2 BETWEEN 'A' AND 'C'""")

    assertEquals(expected, actual)
  }









  @Ignore
  @Test
  def testDateOperator(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .filter("FIELD1 >= 0")
    val actual = sqlTestUtil.translator.translate("""
      SELECT *
      FROM TABLE1
      WHERE FIELD2 < DATE '2015-10-31'""")

    assertEquals(expected, actual)
  }

  @Test
  @Ignore
  def testCustomFunction(): Unit = {
    val test = Thread.currentThread()
      .getContextClassLoader.getResourceAsStream("org/apache/calcite/runtime/SqlFunctions.class")

    Thread.currentThread().getContextClassLoader();

    val clazz = classOf[SqlFunctions]
    val cons = clazz.getDeclaredConstructor()
    cons.setAccessible(true)
    val ins =  cons.newInstance()

    val bytes = InstantiationUtil.serializeObject(ins)


    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .filter("FIELD1 >= 0")
    val actual = sqlTestUtil.translator.translate("""
      SELECT SUBSTRING(FIELD2, 0, 2), TRIM(FIELD2), FIELD2 LIKE '%A%',
        FIELD2 LIKE '%A%', ABS(FIELD1), FIELD2 || FIELD2
      FROM TABLE1""")

    assertEquals(expected, actual)
  }
}