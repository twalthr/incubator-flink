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

package org.apache.flink.api.table.sql.calcite

import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.table.expressions._

import scala.collection.JavaConversions._

class RexToExpr private (
    input1Fields: Seq[(String, TypeInformation[_])],
    input2Fields: Seq[(String, TypeInformation[_])] = null)
  extends RexVisitor[Expression] {

  override def visitInputRef(inputRef: RexInputRef): Expression = {
    val index = inputRef.getIndex
    // input 1
    if (index < input1Fields.size) {
      val fieldName = input1Fields(index)._1
      val fieldType = input1Fields(index)._2
      ResolvedFieldReference(fieldName, fieldType)
    }
    // input 2
    else {
      val fieldName = input2Fields(index - input1Fields.size)._1
      val fieldType = input2Fields(index - input1Fields.size)._2
      ResolvedFieldReference(fieldName, fieldType)
    }
  }

  override def visitFieldAccess(fieldAccess: RexFieldAccess): Expression = ???

  override def visitLiteral(literal: RexLiteral): Expression = {
    literal.getType.getSqlTypeName match {
      case SqlTypeName.VARCHAR | SqlTypeName.CHAR =>
        Literal(literal.getValue2, BasicTypeInfo.STRING_TYPE_INFO)
      case SqlTypeName.INTEGER =>
        Literal(literal.getValue2.asInstanceOf[Long].toInt, BasicTypeInfo.INT_TYPE_INFO)
      case _ => ???
    }
  }

  override def visitCorrelVariable(correlVariable: RexCorrelVariable): Expression = ???

  override def visitLocalRef(localRef: RexLocalRef): Expression = ???

  override def visitRangeRef(rangeRef: RexRangeRef): Expression = ???

  override def visitDynamicParam(dynamicParam: RexDynamicParam): Expression = ???

  override def visitCall(call: RexCall): Expression = {
    val operands = call.getOperands.map(_.accept(this))
    if (operands.size == 2) {
      translateBinaryCall(call, operands(0), operands(1))
    }
    else if (operands.size == 1) {
      translateUnaryCall(call, operands(0))
    }
    else ???
  }

  override def visitOver(over: RexOver): Expression = ???

  // ----------------------------------------------------------------------------------------------

  def translateBinaryCall(call: RexCall, left: Expression, right: Expression): Expression = {
    call.getKind match {
      case EQUALS =>
        val autoCasted = autoCastBinaryArithmetic(left, right)
        EqualTo(autoCasted._1, autoCasted._2)
      case PLUS =>
        val autoCasted = autoCastBinaryArithmetic(left, right)
        Plus(autoCasted._1, autoCasted._2)
      case MINUS =>
        val autoCasted = autoCastBinaryArithmetic(left, right)
        Minus(autoCasted._1, autoCasted._2)
      case TIMES =>
        val autoCasted = autoCastBinaryArithmetic(left, right)
        Mul(autoCasted._1, autoCasted._2)
      case DIVIDE =>
        val autoCasted = autoCastBinaryArithmetic(left, right)
        Div(autoCasted._1, autoCasted._2)
      case _ => ???
    }
  }

  def translateUnaryCall(call: RexCall, operand: Expression): Expression = {
    call.getKind match {
      case CAST => Cast(operand, TypeConverter.sqlTypeToTypeInfo(call.getType.getSqlTypeName))
      case _ => ???
    }
  }

  def autoCastBinaryArithmetic(o1: Expression, o2: Expression): (Expression, Expression) = {
    if (o1.typeInfo != o2.typeInfo && o1.typeInfo.isBasicType && o2.typeInfo.isBasicType) {
      if (o1.typeInfo.asInstanceOf[BasicTypeInfo[_]].shouldAutocastTo(
        o2.typeInfo.asInstanceOf[BasicTypeInfo[_]])) {
        (Cast(o1, o2.typeInfo), o2)
      } else if (o2.typeInfo.asInstanceOf[BasicTypeInfo[_]].shouldAutocastTo(
        o1.typeInfo.asInstanceOf[BasicTypeInfo[_]])) {
        (o1, Cast(o2, o1.typeInfo))
      } else {
        (o1, o2)
      }
    }
    else {
      (o1, o2)
    }
  }

}

object RexToExpr {

  def translate(rexNode: RexNode, inputFields: Seq[(String, TypeInformation[_])]): Expression = {
    rexNode.accept(new RexToExpr(inputFields))
  }

  def translate(
      rexNode: RexNode,
      input1Fields: Seq[(String, TypeInformation[_])],
      input2Fields: Seq[(String, TypeInformation[_])]): Expression = {
    rexNode.accept(new RexToExpr(input1Fields, input2Fields))
  }

}
