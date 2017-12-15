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
  * Encoding descriptor for comma-separated values (CSV).
  */
class CSV extends EncodingDescriptor("csv") {

  private var fieldDelim: Option[String] = None
  private var lineDelim: Option[String] = None
  private val encodingSchema: mutable.LinkedHashMap[String, String] =
      mutable.LinkedHashMap[String, String]()
  private var quoteCharacter: Option[Character] = None
  private var commentPrefix: Option[String] = None
  private var isIgnoreFirstLine: Option[Boolean] = None
  private var lenient: Option[Boolean] = None

  /**
    * Sets the field delimiter, "," by default.
    *
    * @param delim the field delimiter
    */
  def fieldDelimiter(delim: String): CSV = {
    this.fieldDelim = Some(delim)
    this
  }

  /**
    * Sets the line delimiter, "\n" by default.
    *
    * @param delim the line delimiter
    */
  def lineDelimiter(delim: String): CSV = {
    this.lineDelim = Some(delim)
    this
  }

  /**
    * Sets the encoding schema with field names and the types. Required.
    * The table schema must not contain nested fields.
    *
    * This method overwrites existing fields added with [[field()]].
    *
    * @param schema the table schema
    */
  def schema(schema: TableSchema): CSV = {
    this.encodingSchema.clear()
    NormalizedProperties.normalizeTableSchema(schema).foreach {
      case (n, t) => field(n, t)
    }
    this
  }

  /**
    * Adds an encoding field with the field name and the type information. Required.
    * This method can be called multiple times. The call order of this method defines
    * also the order of the fields in the encoding.
    *
    * @param fieldName the field name
    * @param fieldType the type information of the field
    */
  def field(fieldName: String, fieldType: TypeInformation[_]): CSV = {
    field(fieldName, NormalizedProperties.normalizeTypeInfo(fieldType))
    this
  }

  /**
    * Adds an encoding field with the field name and the type string. Required.
    * This method can be called multiple times. The call order of this method defines
    * also the order of the fields in the encoding.
    *
    * @param fieldName the field name
    * @param fieldType the type string of the field
    */
  def field(fieldName: String, fieldType: String): CSV = {
    if (encodingSchema.contains(fieldName)) {
      throw new IllegalArgumentException(s"Duplicate field name $fieldName.")
    }
    encodingSchema += (fieldName -> fieldType)
    this
  }

  /**
    * Sets a quote character for String values, null by default.
    *
    * @param quote the quote character
    */
  def quoteCharacter(quote: Character): CSV = {
    this.quoteCharacter = Option(quote)
    this
  }

  /**
    * Sets a prefix to indicate comments, null by default.
    *
    * @param prefix the prefix to indicate comments
    */
  def commentPrefix(prefix: String): CSV = {
    this.commentPrefix = Option(prefix)
    this
  }

  /**
    * Ignore the first line. Not skip the first line by default.
    */
  def ignoreFirstLine(): CSV = {
    this.isIgnoreFirstLine = Some(true)
    this
  }

  /**
    * Skip records with parse error instead to fail. Throw an exception by default.
    */
  def ignoreParseErrors(): CSV = {
    this.lenient = Some(true)
    this
  }

  /**
    * Internal method for encoding properties conversion.
    */
  override protected def addEncodingProperties(properties: NormalizedProperties): Unit = {
    fieldDelim.foreach(properties.putString("field-delimiter", _))
    lineDelim.foreach(properties.putString("line-delimiter", _))
    properties.putTableSchema("fields", encodingSchema.toIndexedSeq)
    quoteCharacter.foreach(properties.putCharacter("quote-character", _))
    commentPrefix.foreach(properties.putString("comment-prefix", _))
    isIgnoreFirstLine.foreach(properties.putBoolean("ignore-first-line", _))
    lenient.foreach(properties.putBoolean("ignore-parse-errors", _))
  }
}

/**
  * Encoding descriptor for comma-separated values (CSV).
  */
object CSV {

  /**
    * Encoding descriptor for comma-separated values (CSV).
    */
  def apply(): CSV = new CSV()

}
