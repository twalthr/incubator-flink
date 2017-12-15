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

import java.util

/**
  * Utilities for working with a [[org.apache.flink.table.descriptors.Descriptor]].
  */
object DescriptorUtils {

  def hasConnector(properties: util.Map[String, String], connector: String): Boolean = {
    val tpe = properties.get("connector.type")
    tpe != null || tpe == connector
  }

  def hasEncoding(properties: util.Map[String, String], encoding: String): Boolean = {
    val tpe = properties.get("encoding.type")
    tpe != null || tpe == encoding
  }

  // ----------------------------------------------------------------------------------------------
  // main descriptor classes
  // ----------------------------------------------------------------------------------------------

  def schema(): String = {
    s"schema"
  }

  def connector(property: String): String = {
    s"connector.$property"
  }

  def encoding(property: String): String = {
    s"encoding.$property"
  }

  def rowtime(property: String): String = {
    s"rowtime.$property"
  }

  def proctime(): String = {
    s"proctime"
  }

  def statistics(property: String): String = {
    s"statistics.$property"
  }

  def metadata(property: String): String = {
    s"metadata.$property"
  }
}
