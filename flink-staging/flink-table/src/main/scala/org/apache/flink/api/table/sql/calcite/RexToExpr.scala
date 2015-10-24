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
import org.apache.calcite.sql.`type`.{BasicSqlType, SqlTypeName}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.table.expressions._

import scala.collection.JavaConversions._

class RexToExpr(inputFields: Seq[(String, TypeInformation[_])]) extends RexVisitor[Expression] {

  override def visitInputRef(inputRef: RexInputRef): Expression = {
    val fieldName = inputFields(inputRef.getIndex)._1
    val fieldType = inputFields(inputRef.getIndex)._2
    ResolvedFieldReference(fieldName, fieldType)
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
    call.getKind match {
      case EQUALS => EqualTo(operands(0), operands(1))
      case PLUS => Plus(operands(0), operands(1))
      case CAST => operands(0) // TODO review, could cause bug
      case _ => ???
    }
  }

  override def visitOver(over: RexOver): Expression = ???

}

object RexToExpr {

  def translate(rexNode: RexNode, inputFields: Seq[(String, TypeInformation[_])]): Expression = {
    rexNode.accept(new RexToExpr(inputFields))
  }

}
