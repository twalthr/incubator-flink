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

import java.util.{ServiceConfigurationError, ServiceLoader}

import org.apache.flink.table.api.{AmbiguousTableSourceException, NoMatchingTableSourceException, TableException}
import org.apache.flink.table.descriptors.{NormalizedProperties, TableSourceDescriptor}
import org.apache.flink.table.util.Logging

import scala.collection.JavaConverters._

/**
  * Service provider interface for finding suitable table source factories for the given properties.
  */
object TableSourceFactoryService extends Logging {

  private lazy val loader = ServiceLoader.load(classOf[TableSourceFactory[_]])

  def findTableSourceFactory(descriptor: TableSourceDescriptor): TableSource[_] = {
    val normalizedProps = new NormalizedProperties()
    descriptor.addProperties(normalizedProps)
    val properties = normalizedProps.getProperties
    val javaProperties = properties.asJava

    var matchingFactory: Option[TableSourceFactory[_]] = None
    try {
      val iter = loader.iterator()
      while (iter.hasNext) {
        val factory = iter.next()
        val isMatch = try {
          factory.matches(javaProperties)
        } catch {
          case t: Throwable =>
            throw new TableException(
              s"Table source factory '${factory.getClass.getCanonicalName}' caused an exception.",
              t)
        }
        if (isMatch) {
          matchingFactory match {
            case Some(_) => throw new AmbiguousTableSourceException(properties)
            case None => matchingFactory = Some(factory)
          }
        }
      }
    } catch {
      case e: ServiceConfigurationError =>
        LOG.error("Could not load service provider for table source factories.", e)
        throw new TableException("Could not load service provider for table source factories.", e)
    }
    val factory = matchingFactory.getOrElse(throw new NoMatchingTableSourceException(properties))
    try {
      factory.create(javaProperties)
    } catch {
      case t: Throwable =>
        throw new TableException(
          s"Table source factory '${factory.getClass.getCanonicalName}' caused an exception.",
          t)
    }
  }
}
