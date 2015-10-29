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

import org.junit.Test
import org.junit.Assert._

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._

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
  def testAggregateCountPerGroup(): Unit = {
    val sqlTestUtil = new SqlTestUtil

    val expected = sqlTestUtil.table1
      .groupBy("FIELD1")
      .select("FIELD1, FIELD1.min AS VALMIN, FIELD1.max AS VALMAX, FIELD1.count AS VALCOUNT")
    val actual = sqlTestUtil.translator.translate("""
      SELECT FIELD1, MIN(FIELD1) AS VALMIN, MAX(FIELD1) AS VALMAX, COUNT(FIELD1) AS VALCOUNT
      FROM TABLE1
      GROUP BY FIELD1""")

    assertEquals(expected, actual)
  }
}