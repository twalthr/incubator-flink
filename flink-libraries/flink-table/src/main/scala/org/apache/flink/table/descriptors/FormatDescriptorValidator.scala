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

import org.apache.flink.table.descriptors.FormatDescriptorValidator.{FORMAT_TYPE, FORMAT_PROPERTY_VERSION}

/**
  * Validator for [[FormatDescriptor]].
  */
class FormatDescriptorValidator extends DescriptorValidator {

  override def validate(properties: DescriptorProperties): Unit = {
    properties.validateString(FORMAT_TYPE, isOptional = false, minLen = 1)
    properties.validateInt(FORMAT_PROPERTY_VERSION, isOptional = true, 0, Integer.MAX_VALUE)
  }
}

object FormatDescriptorValidator {

  /**
    * Key for describing the type of the format. Usually used for factory discovery.
    */
  val FORMAT_TYPE = "format.type"

  /**
    *  Key for describing the property version. This property can be used for backwards
    *  compatibility in case the property format changes.
    */
  val FORMAT_PROPERTY_VERSION = "format.property-version"

  /**
    * Key for describing the version of the format. This property can be used for different
    * format versions (e.g. Avro 1.8.2 or Avro 2.0).
    */
  val FORMAT_VERSION = "format.version"

}
