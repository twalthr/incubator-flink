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

import org.apache.flink.table.api.{BatchTableEnvironment, Table, TableException}
import org.apache.flink.table.sources.{BatchTableSource, TableSource, TableSourceFactoryService}

class BatchTableSourceDescriptor(tableEnv: BatchTableEnvironment, schema: Schema)
  extends TableSourceDescriptor {

  schemaDescriptor = Some(schema)

  /**
    * Searches for the specified table source, configures it accordingly, and returns it.
    */
  def toTableSource: TableSource[_] = {
    val source = TableSourceFactoryService.findTableSourceFactory(this)
    source match {
      case _: BatchTableSource[_] => source
      case _ => throw new TableException(
        s"Found table source '${source.getClass.getCanonicalName}' is not applicable " +
          s"in a batch environment.")
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
  def withConnector(connector: ConnectorDescriptor): BatchTableSourceDescriptor = {
    connectorDescriptor = Some(connector)
    this
  }

  /**
    * Specifies an encoding that defines how to read data from a connector.
    */
  def withEncoding(encoding: EncodingDescriptor): BatchTableSourceDescriptor = {
    encodingDescriptor = Some(encoding)
    this
  }
}
