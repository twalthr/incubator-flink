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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableSchema

import scala.collection.mutable

/**
  * Describes a schema of a table.
  */
class Schema extends Descriptor {

  private val tableSchema: mutable.LinkedHashMap[String, String] =
      mutable.LinkedHashMap[String, String]()

  /**
    * Sets the schema with field names and the types. Required.
    *
    * This method overwrites existing fields added with [[field()]].
    *
    * @param schema the table schema
    */
  def schema(schema: TableSchema): Schema = {
    this.tableSchema.clear()
    NormalizedProperties.normalizeTableSchema(schema).foreach {
      case (n, t) => field(n, t)
    }
    this
  }

  /**
    * Adds a field with the field name and the type information. Required.
    * This method can be called multiple times. The call order of this method defines
    * also the order of the fields in a row.
    *
    * @param fieldName the field name
    * @param fieldType the type information of the field
    */
  def field(fieldName: String, fieldType: TypeInformation[_]): Schema = {
    field(fieldName, NormalizedProperties.normalizeTypeInfo(fieldType))
    this
  }

  /**
    * Adds a field with the field name and the type string. Required.
    * This method can be called multiple times. The call order of this method defines
    * also the order of the fields in a row.
    *
    * @param fieldName the field name
    * @param fieldType the type string of the field
    */
  def field(fieldName: String, fieldType: String): Schema = {
    if (tableSchema.contains(fieldName)) {
      throw new IllegalArgumentException(s"Duplicate field name $fieldName.")
    }
    tableSchema += (fieldName -> fieldType)
    this
  }

  /**
    * Internal method for properties conversion.
    */
  final override def addProperties(properties: NormalizedProperties): Unit = {
    properties.putTableSchema(DescriptorUtils.schema(), tableSchema.toIndexedSeq)
  }
}

/**
  * Describes a schema of a table.
  */
object Schema {

  /**
    * Describes a schema of a table.
    */
  def apply(): Schema = new Schema()
}
