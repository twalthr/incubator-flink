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

import org.apache.flink.table.descriptors.DescriptorUtils.connector

/**
  * Describes a connector to an other system.
  *
  * @param tpe string identifier for the connector
  */
abstract class ConnectorDescriptor(private val tpe: String) extends Descriptor {

  /**
    * Internal method for properties conversion.
    */
  final def addProperties(properties: NormalizedProperties): Unit = {
    properties.putString(connector("type"), tpe)
    val connectorProperties = new NormalizedProperties()
    addConnectorProperties(connectorProperties)
    connectorProperties.getProperties.foreach { case (k, v) =>
      properties.putString(connector(k), v)
    }
  }

  /**
    * Internal method for connector properties conversion.
    */
  protected def addConnectorProperties(properties: NormalizedProperties): Unit

}
