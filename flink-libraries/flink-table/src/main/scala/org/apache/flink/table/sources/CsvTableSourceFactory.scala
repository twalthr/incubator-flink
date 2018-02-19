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

import org.apache.flink.table.api.TableException
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.{CONNECTOR_PROPERTY_VERSION, CONNECTOR_TYPE}
import org.apache.flink.table.descriptors.CsvValidator._
import org.apache.flink.table.descriptors.FileSystemValidator.{CONNECTOR_PATH, CONNECTOR_TYPE_VALUE}
import org.apache.flink.table.descriptors.FormatDescriptorValidator.{FORMAT_PROPERTY_VERSION, FORMAT_TYPE}
import org.apache.flink.table.descriptors.SchemaValidator.{SCHEMA, SCHEMA_DERIVE_FIELDS, SCHEMA_FIELDS, SCHEMA_PROPERTY_VERSION}
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row

/**
  * Factory for creating configured instances of [[CsvTableSource]].
  */
class CsvTableSourceFactory extends TableSourceFactory[Row] {

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE)
    context.put(FORMAT_TYPE, FORMAT_TYPE_VALUE)
    context.put(CONNECTOR_PROPERTY_VERSION, "1")
    context.put(FORMAT_PROPERTY_VERSION, "1")
    context.put(SCHEMA_PROPERTY_VERSION, "1")
    context
  }

  override def supportedProperties(): util.List[String] = {
    val properties = new util.ArrayList[String]()
    // connector
    properties.add(CONNECTOR_PATH)
    // format
    properties.add(s"$FORMAT_FIELDS.#.${DescriptorProperties.TYPE}")
    properties.add(s"$FORMAT_FIELDS.#.${DescriptorProperties.NAME}")
    properties.add(FORMAT_FIELD_DELIMITER)
    properties.add(FORMAT_LINE_DELIMITER)
    properties.add(FORMAT_QUOTE_CHARACTER)
    properties.add(FORMAT_COMMENT_PREFIX)
    properties.add(FORMAT_IGNORE_FIRST_LINE)
    properties.add(FORMAT_IGNORE_PARSE_ERRORS)
    properties.add(CONNECTOR_PATH)
    // schema
    properties.add(SCHEMA_DERIVE_FIELDS)
    properties.add(s"$SCHEMA.#.${DescriptorProperties.TYPE}")
    properties.add(s"$SCHEMA.#.${DescriptorProperties.NAME}")
    properties
  }

  override def create(properties: util.Map[String, String]): TableSource[Row] = {
    val params = new DescriptorProperties()
    params.putProperties(properties)

    // validate
    new FileSystemValidator().validate(params)
    new CsvValidator().validate(params)
    new SchemaValidator().validate(params)

    // build
    val csvTableSourceBuilder = new CsvTableSource.Builder

    val formatSchema = params.getTableSchema(FORMAT_FIELDS)
    val tableSchema = params.getTableSchema(SCHEMA_FIELDS).get

    // the CsvTableSource needs some rework first
    // for now the schema must be equal to the encoding
    if (!formatSchema.contains(tableSchema)) {
      throw new TableException(
        "Encodings that differ from the schema are not supported yet for CsvTableSources.")
    }

    params.getString(CONNECTOR_PATH).foreach(csvTableSourceBuilder.path)
    params.getString(FORMAT_FIELD_DELIMITER).foreach(csvTableSourceBuilder.fieldDelimiter)
    params.getString(FORMAT_LINE_DELIMITER).foreach(csvTableSourceBuilder.lineDelimiter)

    formatSchema.foreach { schema =>
      schema.getColumnNames.zip(schema.getTypes).foreach { case (name, tpe) =>
        csvTableSourceBuilder.field(name, tpe)
      }
    }
    params.getCharacter(FORMAT_QUOTE_CHARACTER).foreach(csvTableSourceBuilder.quoteCharacter)
    params.getString(FORMAT_COMMENT_PREFIX).foreach(csvTableSourceBuilder.commentPrefix)
    params.getBoolean(FORMAT_IGNORE_FIRST_LINE).foreach { flag =>
      if (flag) {
        csvTableSourceBuilder.ignoreFirstLine()
      }
    }
    params.getBoolean(FORMAT_IGNORE_PARSE_ERRORS).foreach { flag =>
      if (flag) {
        csvTableSourceBuilder.ignoreParseErrors()
      }
    }

    csvTableSourceBuilder.build()
  }
}
