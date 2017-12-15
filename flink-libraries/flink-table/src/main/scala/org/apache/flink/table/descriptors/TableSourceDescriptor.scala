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

import org.apache.flink.table.descriptors.DescriptorUtils.statistics
import org.apache.flink.table.plan.stats.TableStats

import scala.collection.JavaConverters._

/**
  * Common class for all descriptors describing a table source.
  */
abstract class TableSourceDescriptor extends Descriptor {

  protected var schemaDescriptor: Option[Schema] = None
  protected var connectorDescriptor: Option[ConnectorDescriptor] = None
  protected var encodingDescriptor: Option[EncodingDescriptor] = None
  protected var proctimeDescriptor: Option[Proctime] = None
  protected var rowtimeDescriptor: Option[Rowtime] = None
  protected var statisticsDescriptor: Option[Statistics] = None
  protected var metaDescriptor: Option[Metadata] = None

  /**
    * Internal method for properties conversion.
    */
  override def addProperties(properties: NormalizedProperties): Unit = {
    schemaDescriptor.foreach(_.addProperties(properties))
    connectorDescriptor.foreach(_.addProperties(properties))
    encodingDescriptor.foreach(_.addProperties(properties))
    proctimeDescriptor.foreach(_.addProperties(properties))
    rowtimeDescriptor.foreach(_.addProperties(properties))
    metaDescriptor.foreach(_.addProperties(properties))
  }

  /**
    * Reads table statistics from the descriptors properties.
    */
  protected def getTableStats: Option[TableStats] = {
      val normalizedProps = new NormalizedProperties()
      addProperties(normalizedProps)
      val rowCount = normalizedProps.getLong(statistics("row-count")).map(v => Long.box(v))
      rowCount match {
        case Some(cnt) =>
          val columnStats = normalizedProps.getColumnStats(statistics("columns"))
          Some(TableStats(cnt, columnStats.asJava))
        case None =>
          None
      }
    }
}
