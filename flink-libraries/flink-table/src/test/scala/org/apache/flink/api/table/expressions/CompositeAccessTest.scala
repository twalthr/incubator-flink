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

package org.apache.flink.api.table.expressions

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.expressions.CompositeAccessTest.{MyCaseClass, MyCaseClass2, MyPojo}
import org.apache.flink.api.table.expressions.utils.ExpressionTestBase
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.table._
import org.junit.Test


class CompositeAccessTest  extends ExpressionTestBase {

  @Test
  def testGetField(): Unit = {
    testTableApi(
      'f0.getField("intField"),
      "f0.getField('intField')",
      "42")

    testTableApi(
      'f0.getField("stringField"),
      "f0.getField('stringField')",
      "Hello")
  }

  @Test
  def testSetField(): Unit = {

  }

  @Test
  def testFieldArity(): Unit = {

  }

  @Test
  def testNonCompositeField(): Unit = {

  }

  @Test
  def testWrongKeyField(): Unit = {

  }

  @Test
  def testWrongValueField(): Unit = {

  }

  // ----------------------------------------------------------------------------------------------

  def testData = {
    val testData = new Row(6)
    testData.setField(0, MyCaseClass(42, "Bob", booleanField = true))
    testData.setField(1, MyCaseClass2(MyCaseClass(25, "Timo", booleanField = false)))
    testData.setField(2, ("a", "b"))
    testData.setField(3, new org.apache.flink.api.java.tuple.Tuple2[String, String]("a", "b"))
    testData.setField(4, new MyPojo())
    testData.setField(5, 13)
    testData
  }

  def typeInfo = {
    new RowTypeInfo(Seq(
      createTypeInformation[MyCaseClass],
      createTypeInformation[MyCaseClass2],
      createTypeInformation[(String, String)],
      new TupleTypeInfo(STRING_TYPE_INFO, STRING_TYPE_INFO),
      TypeExtractor.createTypeInfo(classOf[MyPojo]),
      INT_TYPE_INFO)).asInstanceOf[TypeInformation[Any]]
  }

}

object CompositeAccessTest {
  case class MyCaseClass(intField: Int, stringField: String, booleanField: Boolean)

  case class MyCaseClass2(objectField: MyCaseClass)

  class MyPojo {
    private var myInt: Int = 0
    private var myString: String = "Hello"

    def getMyInt = myInt

    def setMyInt(value: Int) = {
      myInt = value
    }

    def getMyString = myString

    def setMyString(value: String) = {
      myString = myString
    }
  }
}
