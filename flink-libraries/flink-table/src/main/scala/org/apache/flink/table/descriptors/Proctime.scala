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

/**
  * Proctime descriptor for describing a processing time attribute in the schema.
  */
class Proctime extends Descriptor {

  private var proctimeName: Option[String] = None

  /**
    * Declares a field of the schema to be the processing time attribute.
    *
    * @param name The name of the field that becomes the processing time field.
    */
  def field(name: String): Proctime = {
    proctimeName = Some(name)
    this
  }

  /**
    * Internal method for properties conversion.
    */
  final override def addProperties(properties: NormalizedProperties): Unit = {
    proctimeName.foreach(properties.putString("proctime", _))
  }
}

/**
  * Proctime descriptor for describing a processing time attribute in the schema.
  */
object Proctime {

  /**
    * Proctime descriptor for describing a processing time attribute in the schema.
    */
  def apply(): Proctime = new Proctime()
}
