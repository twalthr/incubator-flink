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

package org.apache.flink.table.sinks

import java.util.Objects

import org.apache.flink.table.sources.tsextractors.TimestampExtractor
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy

/**
  * Describes a rowtime attribute of a [[TableSink]].
  *
  * @todo This class is not final and might change in future versions.
  *
  * @param attributeName The name of the rowtime attribute.
  */
class RowtimeAttributeInserter(private val attributeName: String) {

  /** Returns the name of the rowtime attribute. */
  def getAttributeName: String = attributeName

  /** Returns the [[TimestampExtractor]] for the attribute. */
  def getTimestampExtractor: TimestampExtractor = timestampExtractor

  /** Returns the [[WatermarkStrategy]] for the attribute. */
  def getWatermarkStrategy: WatermarkStrategy = watermarkStrategy

  override def equals(other: Any): Boolean = other match {
    case that: RowtimeAttributeInserter =>
        Objects.equals(attributeName, that.attributeName) &&
        Objects.equals(timestampExtractor, that.timestampExtractor) &&
        Objects.equals(watermarkStrategy, that.watermarkStrategy)
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hash(attributeName, timestampExtractor, watermarkStrategy)
  }
}
