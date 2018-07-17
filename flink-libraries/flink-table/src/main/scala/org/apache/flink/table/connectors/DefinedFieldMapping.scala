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

package org.apache.flink.table.connectors

import java.util.{Map => JMap}
import javax.annotation.Nullable

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.sources.tsextractors.TimestampExtractor

/**
  * The [[DefinedFieldMapping]] interface provides a mapping for the fields of the table schema to
  * fields of the physical returned type.
  *
  * For [[TableSource]] implementations this interface maps [[TableSource.getTableSchema]] to fields
  * of the physical returned type [[TableSource.getReturnType]].
  *
  * For [[TableSink]] implementations this interface maps [[TableSink.getFieldNames]] and
  * [[TableSink.getFieldTypes]] to fields of the physical output type [[TableSink.getOutputType]].
  *
  * If a table source or sink does not implement this interface, the fields are mapped from schema
  * to the fields of the physical output type in [[TypeInformation]] by name.
  *
  * If the fields cannot or should not be implicitly mapped by name, an explicit mapping can be
  * provided by implementing this interface.
  *
  * If a mapping is provided, all fields must be explicitly mapped.
  */
trait DefinedFieldMapping {

  /**
    * Returns the mapping for the fields of the table schema to fields of the physical returned
    * type.
    *
    * The mapping is done based on field names, e.g., a mapping "name" -> "f1" maps the schema field
    * "name" to the field "f1" of the output type, for example in this case the second field of a
    * [[org.apache.flink.api.java.tuple.Tuple]].
    *
    * The returned mapping must map all fields (except proctime and rowtime fields) to the output
    * type. It can also provide a mapping for fields which are not in the table schema to make
    * fields in the physical [[TypeInformation]] accessible for a [[TimestampExtractor]].
    *
    * @return A mapping from table schema fields to [[TypeInformation]] fields or
    *         null if no mapping is necessary.
    */
  @Nullable
  def getFieldMapping: JMap[String, String]
}
