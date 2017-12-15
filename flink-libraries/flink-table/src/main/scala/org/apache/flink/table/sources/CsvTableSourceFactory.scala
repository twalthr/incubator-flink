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

package org.apache.flink.table.sources

import java.util

import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.descriptors.DescriptorUtils._
import org.apache.flink.table.descriptors.NormalizedProperties
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

/**
  * Factory for creating configured instances of [[CsvTableSource]].
  */
class CsvTableSourceFactory extends TableSourceFactory[Row] {

  override def matches(properties: util.Map[String, String]): Boolean = {
    hasConnector(properties, "filesystem") && hasEncoding(properties, "csv")
  }

  override def create(properties: util.Map[String, String]): TableSource[Row] = {
    val params = new NormalizedProperties(properties.asScala)
    val csvTableSourceBuilder = new CsvTableSource.Builder

    val tableSchema = params.getTableSchema(schema())
      .getOrElse(throw new ValidationException("A schema is missing."))

    // the CsvTableSource needs some rework first
    if (params.hasPrefix("proctime") || params.hasPrefix("rowtime")) {
      throw new TableException("Time attributes are not yet supported for CsvTableSources.")
    }
    val encodingSchema = params.getTableSchema(encoding("fields"))

    // for now the schema must be equal to the encoding
    if (!encodingSchema.contains(tableSchema)) {
      throw new TableException(
        "Encodings that differ from the schema are not supported yet for CsvTableSources.")
    }

    params.getString(connector("path")).foreach(csvTableSourceBuilder.path)
    params.getString(encoding("field-delimiter")).foreach(csvTableSourceBuilder.fieldDelimiter)
    params.getString(encoding("line-delimiter")).foreach(csvTableSourceBuilder.lineDelimiter)

    encodingSchema.foreach { schema =>
      schema.getColumnNames.zip(schema.getTypes).foreach { case (name, tpe) =>
        csvTableSourceBuilder.field(name, tpe)
      }
    }
    params.getCharacter(encoding("quote-character")).foreach(csvTableSourceBuilder.quoteCharacter)
    params.getString(encoding("comment-prefix")).foreach(csvTableSourceBuilder.commentPrefix)
    params.getBoolean(encoding("ignore-first-line")).foreach { flag =>
      if (flag) {
        csvTableSourceBuilder.ignoreFirstLine()
      }
    }
    params.getBoolean(encoding("ignore-parse-errors")).foreach { flag =>
      if (flag) {
        csvTableSourceBuilder.ignoreParseErrors()
      }
    }

    csvTableSourceBuilder.build()
  }
}
