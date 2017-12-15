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

package org.apache.flink.table.descriptors

import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableSchema

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * Encoding descriptor for JSON.
  */
class JSON extends EncodingDescriptor("json") {

  private val encodingSchema: mutable.LinkedHashMap[String, String] =
      mutable.LinkedHashMap[String, String]()
  private var fieldMapping: Option[util.Map[String, String]] = None
  private var failOnMissingField: Option[Boolean] = None

  /**
    * Sets the JSON schema with field names and the types for the JSON-encoded input.
    * The JSON schema must not contain nested fields.
    *
    * This method overwrites existing fields added with [[field()]].
    *
    * @param schema the table schema
    */
  def schema(schema: TableSchema): JSON = {
    this.encodingSchema.clear()
    NormalizedProperties.normalizeTableSchema(schema).foreach {
      case (n, t) => field(n, t)
    }
    this
  }

  /**
    * Adds a JSON field with the field name and the type information for the JSON-encoding.
    * This method can be called multiple times. The call order of this method defines
    * also the order of the fields in the JSON-encoding.
    *
    * @param fieldName the field name
    * @param fieldType the type information of the field
    */
  def field(fieldName: String, fieldType: TypeInformation[_]): JSON = {
    field(fieldName, NormalizedProperties.normalizeTypeInfo(fieldType))
    this
  }

  /**
    * Adds a JSON field with the field name and the type string for the JSON-encoding.
    * This method can be called multiple times. The call order of this method defines
    * also the order of the fields in the JSON-encoding.
    *
    * @param fieldName the field name
    * @param fieldType the type string of the field
    */
  def field(fieldName: String, fieldType: String): JSON = {
    if (encodingSchema.contains(fieldName)) {
      throw new IllegalArgumentException(s"Duplicate field name $fieldName.")
    }
    encodingSchema += (fieldName -> fieldType)
    this
  }

  /**
    * Sets a mapping from schema fields to fields of the JSON schema.
    *
    * A field mapping is required if the fields of produced tables should be named different than
    * the fields of the JSON records.
    * The key of the provided Map refers to the field of the table schema,
    * the value to the field in the JSON schema.
    *
    * @param tableToJsonMapping A mapping from table schema fields to JSON schema fields.
    * @return The builder.
    */
  def tableToJsonMapping(tableToJsonMapping: util.Map[String, String]): JSON = {
    this.fieldMapping = Some(tableToJsonMapping)
    this
  }

  /**
    * Sets flag whether to fail if a field is missing or not.
    *
    * @param failOnMissingField If set to true, the operation fails if there is a missing field.
    *                           If set to false, a missing field is set to null.
    * @return The builder.
    */
  def failOnMissingField(failOnMissingField: Boolean): JSON = {
    this.failOnMissingField = Some(failOnMissingField)
    this
  }

  /**
    * Internal method for encoding properties conversion.
    */
  override protected def addEncodingProperties(properties: NormalizedProperties): Unit = {
    properties.putIndexedFixedProperties(
      "fields",
      Seq("name", "type"),
      encodingSchema.toIndexedSeq.map(t => Seq(t._1, t._2))
    )
    fieldMapping.foreach { m =>
      properties.putIndexedFixedProperties(
        "table-json-mapping",
        Seq("table-name", "json-name"),
        m.asScala.toSeq.map(t => Seq(t._1, t._2)))
    }
    failOnMissingField.foreach(properties.putBoolean("fail-on-missing-field", _))
  }
}

/**
  * Encoding descriptor for JSON.
  */
object JSON {

  /**
    * Encoding descriptor for JSON.
    */
  def apply(): JSON = new JSON()
}
