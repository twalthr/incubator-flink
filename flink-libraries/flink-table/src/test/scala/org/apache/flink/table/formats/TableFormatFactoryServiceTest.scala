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

package org.apache.flink.table.formats

import java.util.{HashMap => JHashMap, Map => JMap}

import org.apache.flink.table.api.{AmbiguousTableFormatException, NoMatchingTableFormatException}
import org.apache.flink.table.descriptors.FormatDescriptorValidator
import org.apache.flink.table.factories.TableFormatFactory
import org.apache.flink.table.formats.TableFormatFactoryServiceTest.{COMMON_PATH, SPECIAL_PATH, TEST_FORMAT_TYPE, UNIQUE_PROPERTY}
import org.apache.flink.table.formats.utils.{TestAmbiguousTableFormatFactory, TestTableFormatFactory}
import org.junit.Assert.{assertNotNull, assertTrue}
import org.junit.Test

/**
  * Tests for [[TableFormatFactoryService]]. The tests assume the two format factories
  * [[TestTableFormatFactory]] and [[TestAmbiguousTableFormatFactory]] are registered.
  *
  * The first format does not support SPECIAL_PATH but supports schema derivation. The
  * latter format does not support UNIQUE_PROPERTY nor schema derivation. Both formats
  * have the same context and support COMMON_PATH.
  */
class TableFormatFactoryServiceTest {

  @Test
  def testValidProperties(): Unit = {
    val props = properties()
    assertNotNull(
      TableFormatFactoryService.find(classOf[TableFormatFactory[_]], props))
  }

  @Test
  def testDifferentContextVersion(): Unit = {
    val props = properties()
    props.put(FormatDescriptorValidator.FORMAT_PROPERTY_VERSION, "2")
    // for now we support any property version, the property version should not affect the
    // discovery at the moment and thus the format should still be found
    val foundFactory = TableFormatFactoryService.find(classOf[TableFormatFactory[_]], props)
    assertTrue(foundFactory.isInstanceOf[TestTableFormatFactory])
  }

  @Test
  def testAmbiguousMoreSupportSelection(): Unit = {
    val props = properties()
    props.remove(UNIQUE_PROPERTY) // both formats match now
    props.put(SPECIAL_PATH, "/what/ever") // now only TestAmbiguousTableFormatFactory
    assertTrue(
      TableFormatFactoryService
        .find(classOf[TableFormatFactory[_]], props)
        .isInstanceOf[TestAmbiguousTableFormatFactory])
  }

  @Test
  def testAmbiguousClassBasedSelection(): Unit = {
    val props = properties()
    props.remove(UNIQUE_PROPERTY) // both formats match now
    assertTrue(
      TableFormatFactoryService
        // we are looking for a particular class
        .find(classOf[TestAmbiguousTableFormatFactory], props)
        .isInstanceOf[TestAmbiguousTableFormatFactory])
  }

  @Test
  def testAmbiguousSchemaBasedSelection(): Unit = {
    val props = properties()
    props.remove(UNIQUE_PROPERTY) // both formats match now
    // this is unknown to the schema derivation factory
    props.put("schema.unknown-schema-field", "unknown")

    // the format with schema derivation feels not responsible because of this field,
    // but since there is another format that feels responsible, no exception is thrown.
    assertTrue(
      TableFormatFactoryService
        .find(classOf[TableFormatFactory[_]], props)
        .isInstanceOf[TestAmbiguousTableFormatFactory])
  }

  @Test(expected = classOf[NoMatchingTableFormatException])
  def testMissingClass(): Unit = {
    val props = properties()
    // this class is not a valid factory
    TableFormatFactoryService.find(classOf[TableFormatFactoryServiceTest], props)
  }

  @Test(expected = classOf[NoMatchingTableFormatException])
  def testInvalidContext(): Unit = {
    val props = properties()
    // no context specifies this
    props.put(FormatDescriptorValidator.FORMAT_TYPE, "unknown_format_type")
    TableFormatFactoryService.find(classOf[TableFormatFactory[_]], props)
  }

  @Test(expected = classOf[NoMatchingTableFormatException])
  def testUnsupportedProperty(): Unit = {
    val props = properties()
    props.put("format.property_not_defined_by_any_factory", "/new/path")
    TableFormatFactoryService.find(classOf[TableFormatFactory[_]], props)
  }

  @Test(expected = classOf[AmbiguousTableFormatException])
  def testAmbiguousFactory(): Unit = {
    val props = properties()
    props.remove(UNIQUE_PROPERTY) // now both factories match
    TableFormatFactoryService.find(classOf[TableFormatFactory[_]], props)
  }

  private def properties(): JMap[String, String] = {
    val properties = new JHashMap[String, String]()
    properties.put("connector.type", "test")
    properties.put("format.type", TEST_FORMAT_TYPE)
    properties.put(UNIQUE_PROPERTY, "true")
    properties.put("connector.property-version", "1")
    properties.put("format.property-version", "1")
    properties.put(COMMON_PATH, "/path/to/target")
    properties.put("schema.0.name", "a")
    properties.put("schema.1.name", "b")
    properties.put("schema.2.name", "c")
    properties
  }
}

object TableFormatFactoryServiceTest {

  val TEST_FORMAT_TYPE = "test-format"
  val COMMON_PATH = "format.common-path"
  val SPECIAL_PATH = "format.special-path"
  val UNIQUE_PROPERTY = "format.unique-property"
}
