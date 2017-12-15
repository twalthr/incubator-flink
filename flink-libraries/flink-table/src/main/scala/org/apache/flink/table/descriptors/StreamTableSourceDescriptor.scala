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

import org.apache.flink.table.api.{StreamTableEnvironment, Table, TableException}
import org.apache.flink.table.sources.{StreamTableSource, TableSource, TableSourceFactoryService}

/**
  * Descriptor for specifying a table source in a streaming environment.
  */
class StreamTableSourceDescriptor(
    tableEnv: StreamTableEnvironment,
    schema: Schema)
  extends TableSourceDescriptor {

  schemaDescriptor = Some(schema)

  /**
    * Searches for the specified table source, configures it accordingly, and returns it.
    */
  def toTableSource: TableSource[_] = {
    val source = TableSourceFactoryService.findTableSourceFactory(this)
    source match {
      case _: StreamTableSource[_] => source
      case _ => throw new TableException(
        s"Found table source '${source.getClass.getCanonicalName}' is not applicable " +
          s"in a streaming environment.")
    }
  }

  /**
    * Searches for the specified table source, configures it accordingly, and returns it as a table.
    */
  def toTable: Table = {
    tableEnv.fromTableSource(toTableSource)
  }

  /**
    * Searches for the specified table source, configures it accordingly, and registers it as
    * a table under the given name.
    *
    * @param name table name to be registered in the table environment
    */
  def register(name: String): Unit = {
    tableEnv.registerTableSource(name, toTableSource)
  }

  /**
    * Specifies an connector for reading data from a connector.
    */
  def withConnector(connector: ConnectorDescriptor): StreamTableSourceDescriptor = {
    connectorDescriptor = Some(connector)
    this
  }

  /**
    * Specifies an encoding that defines how to read data from a connector.
    */
  def withEncoding(encoding: EncodingDescriptor): StreamTableSourceDescriptor = {
    encodingDescriptor = Some(encoding)
    this
  }

  /**
    * Specifies a proctime (processing time) attribute in the schema.
    */
  def withProctime(proctime: Proctime): StreamTableSourceDescriptor = {
    proctimeDescriptor = Some(proctime)
    this
  }

  /**
    * Specifies a rowtime (event-time time) attribute in the schema.
    */
  def withRowtime(rowtime: Rowtime): StreamTableSourceDescriptor = {
    rowtimeDescriptor = Some(rowtime)
    this
  }
}
