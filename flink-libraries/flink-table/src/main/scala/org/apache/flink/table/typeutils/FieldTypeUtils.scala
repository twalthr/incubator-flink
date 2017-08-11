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

package org.apache.flink.table.typeutils

import java.lang.reflect.Modifier

import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils.{GenericTypeInfo, PojoTypeInfo, RowTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.expressions.{Alias, Expression, TimeAttribute, UnresolvedFieldReference}
import org.apache.flink.table.sources.{DefinedFieldNames, TableSource}
import org.apache.flink.types.Row

object FieldTypeUtils {

  /**
    * Returns field names and field positions for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation extract the field names and positions from.
    * @tparam A The type of the TypeInformation.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  def getFieldInfo[A](inputType: TypeInformation[A])
    : (Array[String], Array[Int]) = {

    validateInputType(inputType)

    (getFieldNames(inputType), getFieldIndices(inputType))
  }

  /**
    * Returns field names and field positions for a given [[TypeInformation]] and [[Array]] of
    * [[Expression]]. It does not handle time attributes but considers them in indices.
    *
    * @param inputType The [[TypeInformation]] against which the [[Expression]]s are evaluated.
    * @param exprs     The expressions that define the field names.
    * @tparam A The type of the TypeInformation.
    * @return A tuple of two arrays holding the field names and corresponding field positions.
    */
  def getFieldInfo[A](
      inputType: TypeInformation[A],
      exprs: Array[Expression])
    : (Array[String], Array[Int]) = {

   validateInputType(inputType)

    val indexedNames: Array[(Int, String)] = inputType match {
      case a: AtomicType[_] =>
        exprs.zipWithIndex flatMap {
          case (UnresolvedFieldReference(name), idx) =>
            if (idx > 0) {
              throw new TableException("Table of atomic type can only have a single field.")
            }
            Some((0, name))
          case _ => throw new TableException("Field reference expression requested.")
        }
      case t: TupleTypeInfo[A] =>
        exprs.zipWithIndex flatMap {
          case (UnresolvedFieldReference(name), idx) =>
            Some((idx, name))
          case (Alias(UnresolvedFieldReference(origName), name, _), _) =>
            val idx = t.getFieldIndex(origName)
            if (idx < 0) {
              throw new TableException(s"$origName is not a field of type $t")
            }
            Some((idx, name))
          case (_: TimeAttribute, _) =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }
      case c: CaseClassTypeInfo[A] =>
        exprs.zipWithIndex flatMap {
          case (UnresolvedFieldReference(name), idx) =>
            Some((idx, name))
          case (Alias(UnresolvedFieldReference(origName), name, _), _) =>
            val idx = c.getFieldIndex(origName)
            if (idx < 0) {
              throw new TableException(s"$origName is not a field of type $c")
            }
            Some((idx, name))
          case (_: TimeAttribute, _) =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }
      case p: PojoTypeInfo[A] =>
        exprs flatMap {
          case (UnresolvedFieldReference(name)) =>
            val idx = p.getFieldIndex(name)
            if (idx < 0) {
              throw new TableException(s"$name is not a field of type $p")
            }
            Some((idx, name))
          case Alias(UnresolvedFieldReference(origName), name, _) =>
            val idx = p.getFieldIndex(origName)
            if (idx < 0) {
              throw new TableException(s"$origName is not a field of type $p")
            }
            Some((idx, name))
          case _: TimeAttribute =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }
      case r: RowTypeInfo => {
        exprs.zipWithIndex flatMap {
          case (UnresolvedFieldReference(name), idx) =>
            Some((idx, name))
          case (Alias(UnresolvedFieldReference(origName), name, _), _) =>
            val idx = r.getFieldIndex(origName)
            if (idx < 0) {
              throw new TableException(s"$origName is not a field of type $r")
            }
            Some((idx, name))
          case (_: TimeAttribute, _) =>
            None
          case _ => throw new TableException(
            "Field reference expression or alias on field expression expected.")
        }

      }
      case tpe => throw new TableException(
        s"Source of type $tpe cannot be converted into Table.")
    }

    val (fieldIndexes, fieldNames) = indexedNames.unzip

    if (fieldNames.contains("*")) {
      throw new TableException("Field name can not be '*'.")
    }

    (fieldNames, fieldIndexes)
  }

  /**
    * Returns field names for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation extract the field names.
    * @tparam A The type of the TypeInformation.
    * @return An array holding the field names
    */
  def getFieldNames[A](inputType: TypeInformation[A]): Array[String] = {
    validateType(inputType)

    val fieldNames: Array[String] = inputType match {
      case t: CompositeType[_] => t.getFieldNames
      case a: AtomicType[_] => Array("f0")
      case tpe =>
        throw new TableException(s"Currently only CompositeType and AtomicType are supported. " +
          s"Type $tpe lacks explicit field naming")
    }

    if (fieldNames.contains("*")) {
      throw new TableException("Field name can not be '*'.")
    }

    fieldNames
  }

  /**
    * Validate if class represented by the typeInfo is static and globally accessible
    * @param typeInfo type to check
    * @throws TableException if type does not meet these criteria
    */
  def validateType(typeInfo: TypeInformation[_]): Unit = {
    val clazz = typeInfo.getTypeClass
    if ((clazz.isMemberClass && !Modifier.isStatic(clazz.getModifiers)) ||
      !Modifier.isPublic(clazz.getModifiers) ||
      clazz.getCanonicalName == null) {
      throw TableException(s"Class '$clazz' described in type information '$typeInfo' must be " +
        s"static and globally accessible.")
    }
  }

  /**
    * Validate if type information is a valid input type.
    *
    * @param typeInfo type to check
    * @throws TableException if type does not meet these criteria
    */
  def validateInputType(typeInfo: TypeInformation[_]): Unit = {
    val clazz = typeInfo.getTypeClass
    if ((clazz.isMemberClass && !Modifier.isStatic(clazz.getModifiers)) ||
      !Modifier.isPublic(clazz.getModifiers) ||
      clazz.getCanonicalName == null) {
      throw TableException(s"Class '$clazz' described in type information '$typeInfo' must be " +
        s"static and globally accessible.")
    }

    if (typeInfo.isInstanceOf[GenericTypeInfo[_]] && typeInfo.getTypeClass == classOf[Row]) {
      throw new TableException(
        "A type of GenericTypeInfo<Row> has no information about a row's fields. " +
          "Please specify the type with Types.ROW(...).")
    }
  }

  /**
    * Returns field indexes for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation extract the field positions from.
    * @return An array holding the field positions
    */
  def getFieldIndices(inputType: TypeInformation[_]): Array[Int] = {
    getFieldNames(inputType).indices.toArray
  }

  /**
    * Returns field types for a given [[TypeInformation]].
    *
    * @param inputType The TypeInformation to extract field types from.
    * @return An array holding the field types.
    */
  def getFieldTypes(inputType: TypeInformation[_]): Array[TypeInformation[_]] = {
    validateType(inputType)

    inputType match {
      case t: CompositeType[_] => 0.until(t.getArity).map(t.getTypeAt(_)).toArray
      case a: AtomicType[_] => Array(a.asInstanceOf[TypeInformation[_]])
      case tpe =>
        throw new TableException(s"Currently only CompositeType and AtomicType are supported.")
    }
  }

  /**
    * Returns field names for a given [[TableSource]].
    *
    * @param tableSource The TableSource to extract field names from.
    * @tparam A The type of the TableSource.
    * @return An array holding the field names.
    */
  def getFieldNames[A](tableSource: TableSource[A]): Array[String] = tableSource match {
      case d: DefinedFieldNames => d.getFieldNames
      case _ => getFieldNames(tableSource.getReturnType)
    }

  /**
    * Returns field indices for a given [[TableSource]].
    *
    * @param tableSource The TableSource to extract field indices from.
    * @tparam A The type of the TableSource.
    * @return An array holding the field indices.
    */
  def getFieldIndices[A](tableSource: TableSource[A]): Array[Int] = tableSource match {
    case d: DefinedFieldNames => d.getFieldIndices
    case _ => getFieldIndices(tableSource.getReturnType)
  }

}
