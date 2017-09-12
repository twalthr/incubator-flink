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

package org.apache.flink.table.client.config

import java.lang.{Long => JLong}
import java.util

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{Types, ValidationException}

import _root_.scala.collection.JavaConversions._
import scala.beans.BeanProperty

trait Validatable {
  def validate()
}

case class ConfigNode(
  @BeanProperty @JsonProperty("output") output: String,
  @BeanProperty @JsonProperty("jar") jar: String,
  @BeanProperty @JsonProperty("watermark_interval") watermarkInterval: JLong,
  @BeanProperty @JsonProperty("defaults") defaults: DefaultsNode)
  extends Validatable {

  def this() = this(null, null, null, null)

  override def validate(): Unit = {
    if (output == null || output.toLowerCase != "kafka") {
      throw ValidationException("Kafka sink is the only supported output yet.")
    }
    if (defaults != null) {
      defaults.validate()
    }
  }
}

case class DefaultsNode(
    @BeanProperty @JsonProperty("kafka") kafka: KafkaDefaultsNode)
  extends Validatable {

  def this() = this(null)

  override def validate(): Unit = {
    if (kafka == null) {
      throw ValidationException("Kafka defaults are required.")
    }
    kafka.validate()
  }
}

case class KafkaDefaultsNode(
  @BeanProperty @JsonProperty("properties") properties: util.Map[String, String],
  @BeanProperty @JsonProperty("output-topic-prefix") outputTopicPrefix: String)
  extends Validatable {

  def this() = this(null, null)

  override def validate(): Unit = {
    if (outputTopicPrefix == null) {
      throw ValidationException("Kafka needs a default output topic prefix.")
    }
  }
}

case class CatalogNode(
  @BeanProperty @JsonProperty("tables") tables: util.List[TableNode],
  @BeanProperty @JsonProperty("functions") functions: util.List[FunctionNode])
  extends Validatable {

  def this() = this(null, null)

  override def validate(): Unit = {
    if (tables == null) {
      throw ValidationException("Schema configuration needs table definitions.")
    }
    tables.foreach(_.validate())
    if (functions != null) {
      functions.foreach(_.validate())
    }
  }
}

case class FunctionNode(
  @BeanProperty @JsonProperty("name") name: String,
  @BeanProperty @JsonProperty("class") clazz: String,
  @BeanProperty @JsonProperty("parameters") parameters: util.List[Object])
  extends Validatable {

  override def validate(): Unit = {
    if (name == null || name.isEmpty) {
      throw ValidationException("Function needs a name.")
    }
    if (clazz == null || clazz.isEmpty) {
      throw ValidationException("Function needs a class.")
    }
  }
}

case class TableNode(
  @BeanProperty @JsonProperty("name") name: String,
  @BeanProperty @JsonProperty("source") source: SourceNode,
  @BeanProperty @JsonProperty("encoding") encoding: EncodingNode,
  @BeanProperty @JsonProperty("rowtime") rowtime: RowtimeNode,
  @BeanProperty @JsonProperty("proctime") proctime: String)
  extends Validatable {

  def this() = this(null, null, null, null, null)

  override def validate(): Unit = {
    if (source == null) {
      throw ValidationException(s"Table '$name' needs a source definition.")
    }
    source.validate()

    if (source.tpe.toLowerCase == "kafka") {
      if (encoding == null) {
        throw ValidationException("Table with Kafka source needs an encoding definition.")
      }
      encoding.validate()
    }

    if (rowtime != null) {
      rowtime.validate()
    }
  }
}

case class SourceNode(
  @BeanProperty @JsonProperty("type") tpe: String,
  @BeanProperty @JsonProperty("class") clazz: String,
  @BeanProperty @JsonProperty("properties") properties: util.Map[String, String])
  extends Validatable {

  def this() = this(null, null)

  override def validate(): Unit = {
    if (tpe == null) {
      throw ValidationException(s"Source needs a type.")
    }

    if (tpe.toLowerCase != "kafka" && tpe.toLowerCase != "demo" && tpe.toLowerCase != "custom") {
      throw ValidationException(s"Unsupported source type.")
    }
    if (tpe.toLowerCase == "custom" && clazz.isEmpty) {
      throw ValidationException(s"Custom table source needs a class.")
    }
  }
}

case class EncodingNode(
  @BeanProperty @JsonProperty("type") tpe: String,
  @BeanProperty @JsonProperty("class") clazz: String,
  @BeanProperty @JsonProperty("schema") schema: Array[FieldNode])
  extends Validatable {

  def this() = this(null, null, null)

  override def validate(): Unit = {
    if (tpe == null) {
      throw ValidationException("Encoding needs a type definition.")
    }
    if (tpe.toLowerCase == "avro") {
      if (clazz == null) {
        throw ValidationException("Avro encoding needs a class definition.")
      }
    } else if (tpe.toLowerCase == "json") {
      if (schema == null) {
        throw ValidationException("Json encoding needs a schema definition.")
      }
    } else {
      throw ValidationException("Unsupported encoding type.")
    }
  }
}

case class RowtimeNode(
  @BeanProperty @JsonProperty("type") tpe: String,
  @BeanProperty @JsonProperty("field") field: String,
  @BeanProperty @JsonProperty("watermark") watermark: WatermarkNode)
  extends Validatable {

  def this() = this(null, null, null)

  override def validate(): Unit = {
    if (tpe == null || tpe.toLowerCase == "ingestion-time") {
      if (watermark != null) {
        throw ValidationException("Ingestion-time does not support a watermark strategy.")
      }
    } else if (tpe.toLowerCase == "event-time") {
      if (watermark == null) {
        throw ValidationException("Event-time requires definition of a watermark strategy.")
      }
      watermark.validate()
    } else {
      throw ValidationException("Invalid time definition.")
    }
  }
}

case class WatermarkNode(
  @BeanProperty @JsonProperty("type") tpe: String,
  @BeanProperty @JsonProperty("lag") lag: JLong)
  extends Validatable {

  def this() = this(null, null)

  override def validate(): Unit = {
    if (tpe == null) {
      throw ValidationException("Watermark strategy needs a type definition.")
    }
    if (tpe.toLowerCase == "ascending")  {
      if (lag != null) {
        throw ValidationException("Watermark strategy does not support a lag definition.")
      }
    } else if (tpe.toLowerCase == "bounded")  {
      if (lag == null) {
        throw ValidationException("Watermark strategy needs a lag definition.")
      }
    }
  }
}

case class FieldNode(
  @BeanProperty @JsonProperty("name") name: String,
  @BeanProperty @JsonProperty("type") tpe: String)
  extends Validatable {

  def this() = this(null, null)

  override def validate(): Unit = {
    if (name == null || name.isEmpty) {
      throw new ValidationException("Field name must be set.")
    }
    if (tpe == null || tpe.isEmpty) {
      throw new ValidationException("Field type must be set.")
    }
    convertType()
  }

  def convertType(): TypeInformation[_] = tpe match {
    case "VARCHAR" => Types.STRING
    case "BOOLEAN" => Types.BOOLEAN
    case "TINYINT" => Types.BYTE
    case "SMALLINT" => Types.SHORT
    case "INT" | "INTEGER" => Types.INT
    case "BIGINT" => Types.LONG
    case "REAL" | "FLOAT" => Types.FLOAT
    case "DOUBLE" => Types.DOUBLE
    case "DECIMAL" => Types.DECIMAL
    case "DATE" => Types.SQL_DATE
    case "TIME" => Types.SQL_TIME
    case "TIMESTAMP" => Types.SQL_TIMESTAMP
    case s@_ => throw new ValidationException(s"Unsupported type 's'.")
  }

}


