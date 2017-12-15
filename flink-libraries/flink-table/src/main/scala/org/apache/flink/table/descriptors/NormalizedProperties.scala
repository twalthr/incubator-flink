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

import java.io.Serializable
import java.lang.{Boolean => JBoolean, Double => JDouble, Long => JLong, Integer => JInt}

import org.apache.commons.codec.binary.Base64
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{TableSchema, ValidationException}
import org.apache.flink.table.descriptors.NormalizedProperties.normalizeTableSchema
import org.apache.flink.table.plan.stats.ColumnStats
import org.apache.flink.table.sources.tsextractors.{ExistingField, StreamRecordTimestamp, TimestampExtractor}
import org.apache.flink.table.sources.wmstrategies.{AscendingTimestamps, BoundedOutOfOrderTimestamps, PreserveWatermarks, WatermarkStrategy}
import org.apache.flink.table.typeutils.TypeStringUtils
import org.apache.flink.util.InstantiationUtil
import org.apache.flink.util.Preconditions.checkNotNull

import scala.collection.mutable

/**
  * Utility class for having a unified string-based representation of Table API related classes
  * such as [[TableSchema]], [[TypeInformation]], [[WatermarkStrategy]], etc.
  */
class NormalizedProperties(
    private val properties: mutable.Map[String, String] = new mutable.HashMap[String, String]()
  ) {

  private def put(key: String, value: String): Unit = {
    if (properties.contains(key)) {
      throw new IllegalStateException("Property already present.")
    }
    properties.put(key, value)
  }

  def putClass(key: String, clazz: Class[_]): Unit = {
    checkNotNull(key)
    checkNotNull(clazz)
    put(key, clazz.getCanonicalName)
  }

  def putString(key: String, str: String): Unit = {
    checkNotNull(key)
    checkNotNull(str)
    put(key, str)
  }

  def putBoolean(key: String, b: Boolean): Unit = {
    checkNotNull(key)
    put(key, b.toString)
  }

  def putLong(key: String, l: Long): Unit = {
    checkNotNull(key)
    put(key, l.toString)
  }

  def putCharacter(key: String, c: Character): Unit = {
    checkNotNull(key)
    checkNotNull(c)
    put(key, c.toString)
  }

  def putTableSchema(key: String, schema: TableSchema): Unit = {
    putTableSchema(key, normalizeTableSchema(schema))
  }

  def putTableSchema(key: String, nameAndType: Seq[(String, String)]): Unit = {
    putIndexedFixedProperties(
      key,
      Seq("name", "type"),
      nameAndType.map(t => Seq(t._1, t._2))
    )
  }

  /**
    * Adds an indexed sequence of properties (with sub-properties) under a common key.
    *
    * For example:
    *
    * schema.fields.0.type = INT, schema.fields.0.name = test
    * schema.fields.1.type = LONG, schema.fields.1.name = test2
    *
    * The arity of each propertyValue must match the arity of propertyKeys.
    */
  def putIndexedFixedProperties(
      key: String,
      propertyKeys: Seq[String],
      propertyValues: Seq[Seq[String]])
    : Unit = {
    checkNotNull(key)
    checkNotNull(propertyValues)
    propertyValues.zipWithIndex.foreach { case (values, idx) =>
      if (values.size != propertyKeys.size) {
        throw new IllegalArgumentException("Values must have same arity as keys.")
      }
      values.zipWithIndex.foreach { case (value, keyIdx) =>
          put(s"$key.$idx.${propertyKeys(keyIdx)}", value)
      }
    }
  }

  /**
    * Adds an indexed mapping of properties under a common key.
    *
    * For example:
    *
    * schema.fields.0.type = INT, schema.fields.0.name = test
    *                             schema.fields.1.name = test2
    *
    * The arity of the propertySets can differ.
    */
  def putIndexedVariableProperties(
      key: String,
      propertySets: Seq[Map[String, String]])
    : Unit = {
    checkNotNull(key)
    checkNotNull(propertySets)
    propertySets.zipWithIndex.foreach { case (propertySet, idx) =>
      propertySet.foreach { case (k, v) =>
        put(s"$key.$idx.$k", v)
      }
    }
  }

  def getString(key: String): Option[String] = {
    properties.get(key)
  }

  def getCharacter(key: String): Option[Character] = getString(key) match {
    case Some(c) =>
      if (c.length != 1) {
        throw new ValidationException(s"The value of $key must only contain one character.")
      }
      Some(c.charAt(0))

    case None => None
  }

  def getBoolean(key: String): Option[Boolean] = getString(key) match {
    case Some(b) => Some(JBoolean.parseBoolean(b))

    case None => None
  }

  def getInt(key: String): Option[Int] = getString(key) match {
    case Some(l) => Some(JInt.parseInt(l))

    case None => None
  }

  def getLong(key: String): Option[Long] = getString(key) match {
    case Some(l) => Some(JLong.parseLong(l))

    case None => None
  }

  def getDouble(key: String): Option[Double] = getString(key) match {
    case Some(d) => Some(JDouble.parseDouble(d))

    case None => None
  }

  def getTableSchema(key: String): Option[TableSchema] = {
    // filter for number of columns
    val fieldCount = properties
      .filterKeys(k => k.startsWith(key) && k.endsWith(".name"))
      .size

    if (fieldCount == 0) {
      return None
    }

    // validate fields and build schema
    val schemaBuilder = TableSchema.builder()
    for (i <- 0 until fieldCount) {
      val name = s"$key.$i.name"
      val tpe = s"$key.$i.type"
      schemaBuilder.field(
        properties.getOrElse(name, throw new ValidationException(s"Invalid table schema. " +
          s"Could not find name for field '$key.$i'.")
        ),
        TypeStringUtils.readTypeInfo(
          properties.getOrElse(tpe, throw new ValidationException(s"Invalid table schema. " +
          s"Could not find type for field '$key.$i'."))
        )
      )
    }
    Some(schemaBuilder.build())
  }

  def getColumnStats(key: String): Map[String, ColumnStats] = {

    // filter for number of columns
    val columnCount = properties
      .filterKeys(k => k.startsWith(key) && k.endsWith(".name"))
      .size

    val stats = for (i <- 0 until columnCount) yield {
      val name = properties.getOrElse(
        s"$key.$i.name",
        throw new ValidationException("Could not find name of property."))

      val stats = ColumnStats(
        getLong(s"$key.$i.distinct-count").map(v => Long.box(v)).orNull,
        getLong(s"$key.$i.null-count").map(v => Long.box(v)).orNull,
        getDouble(s"$key.$i.avg-length").map(v => Double.box(v)).orNull,
        getInt(s"$key.$i.max-length").map(v => Int.box(v)).orNull,
        getDouble(s"$key.$i.max-value").map(v => Double.box(v)).orNull,
        getDouble(s"$key.$i.min-value").map(v => Double.box(v)).orNull
      )

      name -> stats
    }

    stats.toMap
  }

  def hasPrefix(prefix: String): Boolean = {
    properties.exists(e => e._1.startsWith(prefix))
  }

  def getProperties: Map[String, String] = {
    properties.toMap
  }
}

object NormalizedProperties {

  // the string representation should be equal to SqlTypeName
  def normalizeTypeInfo(typeInfo: TypeInformation[_]): String = {
    checkNotNull(typeInfo)
    TypeStringUtils.writeTypeInfo(typeInfo)
  }

  def normalizeTableSchema(schema: TableSchema): Seq[(String, String)] = {
    schema.getColumnNames.zip(schema.getTypes).map { case (n, t) =>
      (n, normalizeTypeInfo(t))
    }
  }

  def normalizeTimestampExtractor(extractor: TimestampExtractor): Map[String, String] =
    extractor match {
        case existing: ExistingField =>
          Map(
            "timestamp.type" -> "existing-field",
            "timestamp.field" -> existing.getArgumentFields.apply(0))
        case stream: StreamRecordTimestamp =>
          Map("timestamp.type" -> "stream-record")
        case _: TimestampExtractor =>
          Map(
            "timestamp.type" -> "custom",
            "timestamp.class" -> extractor.getClass.getCanonicalName,
            "timestamp.serialized" -> serialize(extractor))
    }

  def normalizeWatermarkStrategy(strategy: WatermarkStrategy): Map[String, String] =
    strategy match {
      case _: AscendingTimestamps =>
        Map("watermark.type" -> "periodic-ascending")
      case bounding: BoundedOutOfOrderTimestamps =>
        Map(
          "watermark.type" -> "periodic-bounding",
          "watermark.delay" -> bounding.delay.toString)
      case _: PreserveWatermarks =>
        Map("watermark.type" -> "preserving")
      case _: WatermarkStrategy =>
        Map(
          "watermark.type" -> "custom",
          "watermark.class" -> strategy.getClass.getCanonicalName,
          "watermark.serialized" -> serialize(strategy))
    }

  def normalizeColumnStats(columnStats: ColumnStats): Map[String, String] = {
    val stats = mutable.HashMap[String, String]()
    if (columnStats.ndv != null) {
      stats += "distinct-count" -> columnStats.ndv.toString
    }
    if (columnStats.nullCount != null) {
      stats += "null-count" -> columnStats.nullCount.toString
    }
    if (columnStats.avgLen != null) {
      stats += "avg-length" -> columnStats.avgLen.toString
    }
    if (columnStats.maxLen != null) {
      stats += "max-length" -> columnStats.maxLen.toString
    }
    if (columnStats.max != null) {
      stats += "max-value" -> columnStats.max.toString
    }
    if (columnStats.min != null) {
      stats += "min-value" -> columnStats.min.toString
    }
    stats.toMap
  }

  private def serialize(obj: Serializable): String = {
    try {
      val byteArray = InstantiationUtil.serializeObject(obj)
      Base64.encodeBase64URLSafeString(byteArray)
    } catch {
      case e: Exception =>
        throw new ValidationException(
          s"Unable to serialize class '${obj.getClass.getCanonicalName}'.", e)
    }
  }
}
