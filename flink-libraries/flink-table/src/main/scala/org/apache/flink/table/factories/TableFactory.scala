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

package org.apache.flink.table.factories

import java.util

/**
  * A factory to create different table-related instances from string-based properties. This
  * factory is used with Java's Service Provider Interfaces (SPI) for discovering. A factory is
  * called with a set of normalized properties that describe the desired format. The factory allows
  * for matching to the given set of properties.
  *
  * Classes that implement this interface can be added to the
  * "META_INF/services/org.apache.flink.table.factories.TableFactory" file of a JAR file in
  * the current classpath to be found.
  */
trait TableFactory {

  /**
    * Specifies the context that this factory has been implemented for.
    *
    * Typical properties might be:
    *   - connector.type
    *   - format.type
    *
    * Specified property versions allow the framework to provide backwards compatible properties
    * in case of string format changes:
    *   - connector.property-version
    *   - format.property-version
    *
    * An empty context means that the factory matches for all requests.
    */
  def requiredContext(): util.Map[String, String]

  /**
    * List of property keys that this factory can handle. This method will be used for validation.
    * If a property is passed that this factory cannot handle, an exception will be thrown. The
    * list must not contain the keys that are specified by the context.
    *
    * Example properties might be:
    *   - format.line-delimiter
    *   - format.ignore-parse-errors
    *   - format.fields.#.type
    *   - format.fields.#.name
    *
    * Note: Use "#" to denote an array of values where "#" represents one or more digits. Property
    * versions like "format.property-version" must not be part of the supported properties.
    *
    * In some cases it might be useful to declare wildcards "*". Wildcards can only be declared at
    * the end of a property key.
    *
    * For example, if an arbitrary format should be supported:
    *   - format.*
    *
    * Note: Wildcards should be used with caution as they might swallow unsupported properties
    * and thus might lead to undesired behavior.
    */
  def supportedProperties(): util.List[String]
}
