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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableSchema

/**
  * The [[DefinedFieldMapping]] interface provides a mapping for the fields of the table schema
  * ([[TableSource.getTableSchema]] to fields of the physical returned type
  * [[TableSource.getReturnType]] of a [[TableSource]].
  *
  * If a [[TableSource]] does not implement the [[DefinedFieldMapping]] interface, the fields of
  * its [[TableSchema]] are mapped to the fields of its return type [[TypeInformation]] by name.
  *
  * If the fields cannot or should not be implicitly mapped by name, an explicit mapping can be
  * provided by implementing this interface.
  *
  * If a mapping is provided, all fields must be explicitly mapped.
  *
  * @deprecated Use [[org.apache.flink.table.connectors.DefinedFieldMapping]] instead.
  */
@Deprecated
@deprecated
trait DefinedFieldMapping extends org.apache.flink.table.connectors.DefinedFieldMapping
