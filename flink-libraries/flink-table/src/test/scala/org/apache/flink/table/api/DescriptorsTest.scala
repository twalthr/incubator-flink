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

package org.apache.flink.table.api

import _root_.java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.descriptors._
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.utils.TableTestBase
import org.junit.Assert.assertEquals
import org.junit.Test

class DescriptorsTest extends TableTestBase {

  @Test
  def testFileSystem(): Unit = {
    val desc = FileSystem().path("/myfile")
    val expected = Seq(
      "connector.type" -> "filesystem",
      "connector.path" -> "/myfile")
    verifyProperties(desc, expected)
  }

  @Test
  def testJson(): Unit = {
    val map = new util.HashMap[String, String]()
    map.put("whatever", "jsonWhatever")
    val desc = JSON()
      .field("whatever", Types.DECIMAL)
      .failOnMissingField(true)
      .field("jsonWhatever", Types.INT)
      .tableToJsonMapping(map)
    val expected = Seq(
      "encoding.type" -> "json",
      "encoding.fields.0.name" -> "whatever",
      "encoding.fields.0.type" -> "DECIMAL",
      "encoding.fields.1.name" -> "jsonWhatever",
      "encoding.fields.1.type" -> "INT",
      "encoding.fail-on-missing-field" -> "true",
      "encoding.table-json-mapping.0.table-name" -> "whatever",
      "encoding.table-json-mapping.0.json-name" -> "jsonWhatever")
    verifyProperties(desc, expected)
  }

  @Test
  def testCsv(): Unit = {
    val desc = CSV()
      .field("field1", "STRING")
      .field("field2", Types.SQL_TIMESTAMP)
      .field("field3", TypeExtractor.createTypeInfo(classOf[Class[_]]))
      .field("field4", Types.ROW(
        Array[String]("test", "row"),
        Array[TypeInformation[_]](Types.INT, Types.STRING)))
      .lineDelimiter("^")
    val expected = Seq(
      "encoding.type" -> "csv",
      "encoding.fields.0.name" -> "field1",
      "encoding.fields.0.type" -> "STRING",
      "encoding.fields.1.name" -> "field2",
      "encoding.fields.1.type" -> "TIMESTAMP",
      "encoding.fields.2.name" -> "field3",
      "encoding.fields.2.type" -> "ANY(java.lang.Class)",
      "encoding.fields.3.name" -> "field4",
      "encoding.fields.3.type" -> "ROW(test INT, row VARCHAR)",
      "encoding.line-delimiter" -> "^")
    verifyProperties(desc, expected)
  }

  @Test
  def testCsvTableSchema(): Unit = {
    val desc = CSV()
      .schema(new TableSchema(
        Array[String]("test", "row"),
        Array[TypeInformation[_]](Types.INT, Types.STRING)))
      .quoteCharacter('#')
      .ignoreFirstLine()
    val expected = Seq(
      "encoding.type" -> "csv",
      "encoding.fields.0.name" -> "test",
      "encoding.fields.0.type" -> "INT",
      "encoding.fields.1.name" -> "row",
      "encoding.fields.1.type" -> "VARCHAR",
      "encoding.quote-character" -> "#",
      "encoding.ignore-first-line" -> "true")
    verifyProperties(desc, expected)
  }

  @Test
  def testRowtime(): Unit = {
    val desc = Rowtime()
      .field("myField")
      .timestampFromField("otherField")
      .watermarkPeriodicBounding(1000L)
    val expected = Seq(
      "rowtime.0.name" -> "myField",
      "rowtime.0.timestamp.type" -> "existing-field",
      "rowtime.0.timestamp.field" -> "otherField",
      "rowtime.0.watermark.type" -> "periodic-bounding",
      "rowtime.0.watermark.delay" -> "1000"
    )
    verifyProperties(desc, expected)
  }

  @Test
  def testProctime(): Unit = {
    val desc = Proctime()
      .field("myField")
    val expected = Seq(
      "proctime" -> "myField"
    )
    verifyProperties(desc, expected)
  }

  @Test
  def testStatistics(): Unit = {
    val desc = Statistics()
      .rowCount(1000L)
      .columnStats("a", ColumnStats(1L, 2L, 3.0, 4, 5, 6))
      .columnAvgLength("b", 42.0)
      .columnNullCount("a", 300)
    val expected = Seq(
      "statistics.row-count" -> "1000",
      "statistics.columns.0.name" -> "a",
      "statistics.columns.0.distinct-count" -> "1",
      "statistics.columns.0.null-count" -> "300",
      "statistics.columns.0.avg-length" -> "3.0",
      "statistics.columns.0.max-length" -> "4",
      "statistics.columns.0.max-value" -> "5",
      "statistics.columns.0.min-value" -> "6",
      "statistics.columns.1.name" -> "b",
      "statistics.columns.1.avg-length" -> "42.0"
    )
    verifyProperties(desc, expected)
  }

  @Test
  def testStatisticsTableStats(): Unit = {
    val map = new util.HashMap[String, ColumnStats]()
    map.put("a", ColumnStats(null, 2L, 3.0, null, 5, 6))
    val desc = Statistics()
      .tableStats(TableStats(32L, map))
    val expected = Seq(
      "statistics.row-count" -> "32",
      "statistics.columns.0.name" -> "a",
      "statistics.columns.0.null-count" -> "2",
      "statistics.columns.0.avg-length" -> "3.0",
      "statistics.columns.0.max-value" -> "5",
      "statistics.columns.0.min-value" -> "6"
    )
    verifyProperties(desc, expected)
  }

  @Test
  def testMetadata(): Unit = {
    val desc = Metadata()
      .comment("Some additional comment")
      .creationTime(123L)
      .lastAccessTime(12020202L)
    val expected = Seq(
      "metadata.comment" -> "Some additional comment",
      "metadata.creation-time" -> "123",
      "metadata.last-access-time" -> "12020202"
    )
    verifyProperties(desc, expected)
  }

  @Test
  def testStreamTableSourceDescriptor(): Unit = {
    val util = streamTestUtil()
    val desc = util.tableEnv
      .createTable(
        Schema()
          .field("myfield", Types.STRING)
          .field("myfield2", Types.INT))
      .withConnector(
        FileSystem()
          .path("/path/to/csv"))
      .withEncoding(
        CSV()
          .field("myfield", Types.STRING)
          .field("myfield2", Types.INT)
          .quoteCharacter(';')
          .fieldDelimiter("#")
          .lineDelimiter("\r\n")
          .commentPrefix("%%")
          .ignoreFirstLine()
          .ignoreParseErrors())
      .withRowtime(
        Rowtime()
          .field("rowtime")
          .timestampFromDataStream()
          .watermarkFromDataStream())
      .withProctime(
        Proctime()
          .field("myproctime"))
    val expected = Seq(
      "schema.0.name" -> "myfield",
      "schema.0.type" -> "VARCHAR",
      "schema.1.name" -> "myfield2",
      "schema.1.type" -> "INT",
      "connector.type" -> "filesystem",
      "connector.path" -> "/path/to/csv",
      "encoding.type" -> "csv",
      "encoding.fields.0.name" -> "myfield",
      "encoding.fields.0.type" -> "VARCHAR",
      "encoding.fields.1.name" -> "myfield2",
      "encoding.fields.1.type" -> "INT",
      "encoding.quote-character" -> ";",
      "encoding.field-delimiter" -> "#",
      "encoding.line-delimiter" -> "\r\n",
      "encoding.comment-prefix" -> "%%",
      "encoding.ignore-first-line" -> "true",
      "encoding.ignore-parse-errors" -> "true",
      "rowtime.0.name" -> "rowtime",
      "rowtime.0.timestamp.type" -> "stream-record",
      "rowtime.0.watermark.type" -> "preserving",
      "proctime" -> "myproctime"
    )
    verifyProperties(desc, expected)
  }

  private def verifyProperties(descriptor: Descriptor, expected: Seq[(String, String)]): Unit = {
    val normProps = new NormalizedProperties
    descriptor.addProperties(normProps)
    assertEquals(expected.toMap, normProps.getProperties)
  }
}
