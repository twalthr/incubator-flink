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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.table.expressions.utils.ExpressionTestBase
import org.junit.Test


class CompositeAccessTest extends ExpressionTestBase {

  @Test
  def testGetField(): Unit = {
    testSqlApi("testTable._1._2", "42")
    /*testSqlApi("testTable.f0.intField", "42")

    testSqlApi("testTable.f0.stringField", "Bob")

    testSqlApi("testTable.f0.booleanField", "true")

    /*testSqlApi("testTable.f1.objectField.intField", "25")

    // TODO Calcite does not allow this
    // testSqlApi("testTable.f1.objectField", "25")

    testSqlApi("testTable.f1.objectField.stringField", "Timo")

    testSqlApi("testTable.f1.objectField.booleanField", "false")

    testSqlApi("testTable.f2._1", "a")

    testSqlApi("testTable.f3.f1", "b")

    testSqlApi("testTable.f4.myString", "Hello")

    testSqlApi("testTable.f5", "13")

    testSqlApi("testTable.f6", "null")

    testSqlApi("testTable.f7._1", "true")*/

    testSqlApi("testTable.f0", "MyCaseClass(42,Bob,true)")*/
  }

  @Test
  def testWrongKeyField(): Unit = {

  }

  @Test
  def testWrongValueField(): Unit = {

  }

  // ----------------------------------------------------------------------------------------------

  def testData = {
    val testData = ((12, true), "Hello")
    //testData.setField(0, MyCaseClass(42, "Bob", booleanField = true))
    //testData.setField(1, MyCaseClass2(MyCaseClass(25, "Timo", booleanField = false)))
    //testData.setField(2, ("a", "b"))
    //testData.setField(3, new org.apache.flink.api.java.tuple.Tuple2[String, String]("a", "b"))
    //testData.setField(4, new MyPojo())
    //testData.setField(5, 13)
    //testData.setField(6, MyCaseClass2(null))
    //testData.setField(7, Tuple1(true))
    testData
  }

  def typeInfo = {
    createTypeInformation[((Int, Boolean), String)].asInstanceOf[TypeInformation[Any]]
    /*new RowTypeInfo(Seq(
      createTypeInformation[MyCaseClass]
      //createTypeInformation[MyCaseClass2],
      //createTypeInformation[(String, String)],
      //new TupleTypeInfo(STRING_TYPE_INFO, STRING_TYPE_INFO),
      //TypeExtractor.createTypeInfo(classOf[MyPojo]),
      //INT_TYPE_INFO,
      //createTypeInformation[MyCaseClass2],
      //createTypeInformation[Tuple1[Boolean]]
      )).asInstanceOf[TypeInformation[Any]]*/
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
