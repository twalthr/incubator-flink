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

import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.table.{FlinkTypeFactory, ValidationException}
import org.apache.flink.api.table.validate.{ExprValidationResult, ValidationFailure}

case class GetField(child: Expression, key: Any) extends UnaryExpression {

  key match {
    case _: String => // ok
    case _: Int => // ok
    case _ => throw ValidationException("Invalid key type: String or Integer expected.")
  }

  override def toString = s"$child.getField($key)"

  override private[flink] def validateInput(): ExprValidationResult = {
    // check for composite type
    if (!child.resultType.isInstanceOf[CompositeType]) {
      return ValidationFailure(s"Cannot access field of non-composite type '${child.resultType}'.")
    }
    val compositeType = child.resultType.asInstanceOf[CompositeType]

    // check key
    val position = key match {
      case name: String => compositeType.getFieldIndex(str)
      case index: Int => compositeType.getArity
    }
  }

  override private[flink] def resultType: TypeInformation[_] = ???

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode = {
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    relBuilder
      .getRexBuilder
      .makeCast(
        typeFactory.createTypeFromTypeInfo(resultType),
        child.toRexNode)
  }

  override private[flink] def makeCopy(anyRefs: Array[AnyRef]): this.type = {
    val child: Expression = anyRefs.head.asInstanceOf[Expression]
    copy(child, key).asInstanceOf[this.type]
  }


}

case class SetField(composite: Expression, key: Any, value: Expression) extends Expression {

  key match {
    case _: String => // ok
    case _: Int => // ok
    case _ => throw new IllegalArgumentException("Invalid")
  }

  override private[flink] def makeCopy(anyRefs: Array[AnyRef]): this.type = {
    val composite: Expression = anyRefs.head.asInstanceOf[Expression]
    copy(composite, key).asInstanceOf[this.type]
  }

  override def toString = s"$composite.setField($key)"

  override private[flink] def resultType: TypeInformation[_] = ???

  override private[flink] def children: Seq[Expression] = ???
}

case class FieldArity(composite: Expression) extends Expression {

  override def toString = s"$composite.fieldArity"

  override private[flink] def resultType: TypeInformation[_] = ???

  override private[flink] def children: Seq[Expression] = ???
}
