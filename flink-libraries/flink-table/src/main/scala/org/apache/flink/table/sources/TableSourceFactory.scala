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

import java.util

/**
  * A factory to create a [[TableSource]]. This factory is used with Java's Service Provider
  * Interfaces (SPI) for discovering. A factory is called with a set of normalized properties that
  * describe the desired table source. The factory allows for matching to the given set of
  * properties and creating a configured [[TableSource]] accordingly.
  *
  * Classes that implement this interface need to be added to the
  * "META_INF/services/org.apache.flink.table.sources.TableSourceFactory' file of a JAR file in
  * the current classpath to be found.
  */
trait TableSourceFactory[T] {

  /**
    * Returns whether this factory matches the given set of properties and is thus able to
    * create a corresponding [[TableSource]].
    *
    * @param properties normalized properties describing a table source
    * @return flag that indicates of this factory is able to create a [[TableSource]] with
    *         the given properties
    */
  def matches(properties: util.Map[String, String]): Boolean

  /**
    * Creates and configures a [[TableSource]] using the given properties.
    *
    * @param properties normalized properties describing a table source
    * @return the configured table source
    */
  def create(properties: util.Map[String, String]): TableSource[T]

}
