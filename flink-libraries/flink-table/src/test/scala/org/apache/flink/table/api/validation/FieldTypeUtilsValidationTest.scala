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

package org.apache.flink.table.api.validation

import org.apache.flink.api.common.typeinfo.BasicTypeInfo.{DOUBLE_TYPE_INFO, INT_TYPE_INFO, STRING_TYPE_INFO}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, TupleTypeInfo, TypeExtractor}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{CClass, PojoClass, TableException}
import org.apache.flink.table.expressions.{Alias, UnresolvedFieldReference}
import org.apache.flink.table.typeutils.FieldTypeUtils
import org.apache.flink.types.Row
import org.junit.Test

class FieldTypeUtilsValidationTest {

  val tupleType = new TupleTypeInfo(
    INT_TYPE_INFO,
    STRING_TYPE_INFO,
    DOUBLE_TYPE_INFO)

  val caseClassType: TypeInformation[CClass] = implicitly[TypeInformation[CClass]]

  val pojoType: TypeInformation[PojoClass] = TypeExtractor.createTypeInfo(classOf[PojoClass])

  val atomicType = INT_TYPE_INFO

  val genericRowType = new GenericTypeInfo[Row](classOf[Row])


  @Test(expected = classOf[TableException])
  def testGetFieldInfoPojoNames1(): Unit = {
    FieldTypeUtils.getFieldInfo(
      pojoType,
      Array(
        UnresolvedFieldReference("name1"),
        UnresolvedFieldReference("name2"),
        UnresolvedFieldReference("name3")
      ))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoAtomicName2(): Unit = {
    FieldTypeUtils.getFieldInfo(
      atomicType,
      Array(
        UnresolvedFieldReference("name1"),
        UnresolvedFieldReference("name2")
      ))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoTupleAlias3(): Unit = {
    FieldTypeUtils.getFieldInfo(
      tupleType,
      Array(
        Alias(UnresolvedFieldReference("xxx"), "name1"),
        Alias(UnresolvedFieldReference("yyy"), "name2"),
        Alias(UnresolvedFieldReference("zzz"), "name3")
      ))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoCClassAlias3(): Unit = {
    FieldTypeUtils.getFieldInfo(
      caseClassType,
      Array(
        Alias(UnresolvedFieldReference("xxx"), "name1"),
        Alias(UnresolvedFieldReference("yyy"), "name2"),
        Alias(UnresolvedFieldReference("zzz"), "name3")
      ))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoPojoAlias3(): Unit = {
    FieldTypeUtils.getFieldInfo(
      pojoType,
      Array(
        Alias(UnresolvedFieldReference("xxx"), "name1"),
        Alias(UnresolvedFieldReference("yyy"), "name2"),
        Alias(UnresolvedFieldReference("zzz"), "name3")
      ))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoAtomicAlias(): Unit = {
    FieldTypeUtils.getFieldInfo(
      atomicType,
      Array(
        Alias(UnresolvedFieldReference("name1"), "name2")
      ))
  }

  @Test(expected = classOf[TableException])
  def testGetFieldInfoGenericRowAlias(): Unit = {
    FieldTypeUtils.getFieldInfo(
      genericRowType,
      Array(UnresolvedFieldReference("first")))
  }
}
