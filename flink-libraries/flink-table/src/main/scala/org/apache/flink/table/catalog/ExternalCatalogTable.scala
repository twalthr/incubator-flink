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

package org.apache.flink.table.catalog

import java.lang.{Long => JLong}
import java.util.{HashMap => JHashMap, Map => JMap}

import org.apache.flink.table.api.{TableException, TableSchema}
import org.apache.flink.table.catalog.ExternalCatalogTable.{TableTypeConnector, toConnectorDescriptor, toMetadataDescriptor, toStatisticsDescriptor}
import org.apache.flink.table.descriptors.DescriptorUtils.{connector, metadata}
import org.apache.flink.table.descriptors._
import org.apache.flink.table.plan.stats.TableStats

import scala.collection.JavaConverters._

/**
  * Defines a table in an [[ExternalCatalog]].
  */
class ExternalCatalogTable(
    connectorDesc: ConnectorDescriptor,
    encodingDesc: Option[EncodingDescriptor],
    proctimeDesc: Option[Proctime],
    rowtimeDesc: Option[Rowtime],
    statisticsDesc: Option[Statistics],
    metadataDesc: Option[Metadata])
  extends TableSourceDescriptor {

  this.connectorDescriptor = Some(connectorDesc)
  this.encodingDescriptor = encodingDesc
  this.proctimeDescriptor = proctimeDesc
  this.rowtimeDescriptor = rowtimeDesc
  this.statisticsDescriptor = statisticsDesc
  this.metaDescriptor = metadataDesc

  // expose statistics for external table source util
  override def getTableStats: Option[TableStats] = super.getTableStats

  // ----------------------------------------------------------------------------------------------
  // NOTE: the following code is used for backwards compatibility to the TableType approach
  // ----------------------------------------------------------------------------------------------

  /**
    * Returns the legacy table type of an external catalog table.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  lazy val tableType: String = {
    val normalizedProps = new NormalizedProperties()
    connectorDesc.addProperties(normalizedProps)
    normalizedProps
      .getString(connector("legacy-type"))
      .getOrElse(throw new TableException("Could not find a legacy table type to return."))
  }

  /**
    * Returns the legacy schema of an external catalog table.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  lazy val schema: TableSchema = {
    val normalizedProps = new NormalizedProperties()
    connectorDesc.addProperties(normalizedProps)
    normalizedProps
      .getTableSchema(connector("legacy-schema"))
      .getOrElse(throw new TableException("Could not find a legacy schema to return."))
  }

  /**
    * Returns the legacy properties of an external catalog table.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  lazy val properties: JMap[String, String] = {
    val normalizedProps = new NormalizedProperties()
    val legacyProps = new JHashMap[String, String]()
    connectorDesc.addProperties(normalizedProps)
    normalizedProps.getProperties.flatMap { case (k, v) =>
      if (k.startsWith(connector("legacy-property-"))) {
        // remove "connector.legacy-property-"
        Some(legacyProps.put(k.substring(26), v))
      } else {
        None
      }
    }
    legacyProps
  }

  /**
    * Returns the legacy statistics of an external catalog table.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  lazy val stats: TableStats = getTableStats.orNull

  /**
    * Returns the legacy comment of an external catalog table.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  lazy val comment: String = {
    val normalizedProps = new NormalizedProperties()

    metadataDesc match {
      case Some(meta) =>
        meta.addProperties(normalizedProps)
        normalizedProps.getString(metadata("comment")).orNull
      case None =>
        null
    }
  }

  /**
    * Returns the legacy creation time of an external catalog table.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  lazy val createTime: JLong = {
    val normalizedProps = new NormalizedProperties()

    metadataDesc match {
      case Some(meta) =>
        meta.addProperties(normalizedProps)
        normalizedProps.getLong(metadata("creation-time")).map(v => Long.box(v)).orNull
      case None =>
        null
    }
  }

  /**
    * Returns the legacy last access time of an external catalog table.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  lazy val lastAccessTime: JLong = {
    val normalizedProps = new NormalizedProperties()

    metadataDesc match {
      case Some(meta) =>
        meta.addProperties(normalizedProps)
        normalizedProps.getLong(metadata("last-access-time")).map(v => Long.box(v)).orNull
      case None =>
        null
    }
  }

  /**
    * Defines a table in an [[ExternalCatalog]].
    *
    * @param tableType            Table type, e.g csv, hbase, kafka
    * @param schema               Schema of the table (column names and types)
    * @param properties           Properties of the table
    * @param stats                Statistics of the table
    * @param comment              Comment of the table
    * @param createTime           Create timestamp of the table
    * @param lastAccessTime       Timestamp of last access of the table
    * @deprecated Use a descriptor-based constructor instead.
    */
  @Deprecated
  @deprecated("Use a descriptor-based constructor instead.")
  def this(
    tableType: String,
    schema: TableSchema,
    properties: JMap[String, String] = new JHashMap(),
    stats: TableStats = null,
    comment: String = null,
    createTime: JLong = System.currentTimeMillis,
    lastAccessTime: JLong = -1L) = {

    this(
      toConnectorDescriptor(tableType, schema, properties),
      None,
      None,
      None,
      Some(toStatisticsDescriptor(stats)),
      Some(toMetadataDescriptor(comment, createTime, lastAccessTime)))
  }

  /**
    * Returns whether this external catalog table uses the legacy table type.
    *
    * @deprecated for backwards compatibility.
    */
  @Deprecated
  @deprecated("For backwards compatibility.")
  def isLegacyTableType: Boolean = connectorDesc.isInstanceOf[TableTypeConnector]
}

object ExternalCatalogTable {

  /**
    * Defines a table in an [[ExternalCatalog]].
    *
    * @param tableType            Table type, e.g csv, hbase, kafka
    * @param schema               Schema of the table (column names and types)
    * @param properties           Properties of the table
    * @param stats                Statistics of the table
    * @param comment              Comment of the table
    * @param createTime           Create timestamp of the table
    * @param lastAccessTime       Timestamp of last access of the table
    * @deprecated Use a descriptor-based constructor instead.
    */
  @Deprecated
  @deprecated("Use a descriptor-based constructor instead.")
  def apply(
    tableType: String,
    schema: TableSchema,
    properties: JMap[String, String] = new JHashMap(),
    stats: TableStats = null,
    comment: String = null,
    createTime: JLong = System.currentTimeMillis,
    lastAccessTime: JLong = -1L): ExternalCatalogTable = {

    new ExternalCatalogTable(
      tableType,
      schema,
      properties,
      stats,
      comment,
      createTime,
      lastAccessTime)
  }

  class TableTypeConnector(
      tableType: String,
      schema: TableSchema,
      legacyProperties: JMap[String, String])
    extends ConnectorDescriptor("legacy-table-type") {

    override protected def addConnectorProperties(properties: NormalizedProperties): Unit = {
      properties.putString("legacy-type", tableType)
      properties.putTableSchema("legacy-schema", schema)
      legacyProperties.asScala.foreach { case (k, v) =>
          properties.putString(s"legacy-property-$k", v)
      }
    }
  }

  def toConnectorDescriptor(
      tableType: String,
      schema: TableSchema,
      properties: JMap[String, String])
    : ConnectorDescriptor = {

    new TableTypeConnector(tableType, schema, properties)
  }

  def toStatisticsDescriptor(stats: TableStats): Statistics = {
    val statsDesc = Statistics()
    if (stats != null) {
      statsDesc.tableStats(stats)
    }
    statsDesc
  }

  def toMetadataDescriptor(
      comment: String,
      createTime: JLong,
      lastAccessTime: JLong)
    : Metadata = {

    val metadataDesc = Metadata()
    if (comment != null) {
      metadataDesc.comment(comment)
    }
    metadataDesc
      .creationTime(createTime)
      .lastAccessTime(lastAccessTime)
  }
}
