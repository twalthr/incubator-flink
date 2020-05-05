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

package org.apache.flink.table.utils

import scala.collection.JavaConverters._

object RowStringUtils {

  def normalizeRowData(rows: String): String = {
    rows.split("\n")
      .map(normalizeRow)
      .mkString("\n")
  }

  def normalizeRowData(rows: java.util.List[String]): java.util.List[String] = {
    rows.asScala.map(normalizeRow).asJava
  }

  def normalizeRowData(rows: List[String]): List[String] = {
    rows.map(normalizeRow)
  }

  def normalizeRowData(rows: Seq[String]): Seq[String] = {
    rows.map(normalizeRow)
  }

  private def normalizeRow(row: String): String = {
    val normalizedFields = row.replaceAll(",(?!\\s)", ", ")
    s"+I($normalizedFields)"
  }
}
