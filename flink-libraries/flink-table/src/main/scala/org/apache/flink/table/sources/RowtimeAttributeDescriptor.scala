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

import org.apache.flink.table.sources.tsextractors.TimestampExtractor
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy

/**
  * Describes a rowtime attribute of a [[TableSource]].
  *
  * @param attributeName The name of the rowtime attribute.
  * @param timestampExtractor The timestamp extractor to derive the values of the attribute.
  * @param watermarkStrategy The watermark strategy associated with the attribute.
  * @deprecated Use [[RowtimeAttributeExtractor]] for table sources instead.
  */
@Deprecated
@deprecated
class RowtimeAttributeDescriptor(
    attributeName: String,
    timestampExtractor: TimestampExtractor,
    watermarkStrategy: WatermarkStrategy)
  extends RowtimeAttributeExtractor(
    attributeName,
    timestampExtractor,
    watermarkStrategy)
