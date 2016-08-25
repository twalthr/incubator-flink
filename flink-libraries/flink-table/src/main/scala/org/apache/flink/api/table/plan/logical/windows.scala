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

package org.apache.flink.api.table.plan.logical

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.table.{StreamTableEnvironment, TableEnvironment}
import org.apache.flink.api.table.expressions._
import org.apache.flink.api.table.typeutils.{IntervalTypeInfo, TypeCoercion}
import org.apache.flink.api.table.validate.{ValidationFailure, ValidationResult, ValidationSuccess}

abstract class GroupWindow(val alias: Option[Expression]) {
  def resolveExpressions(resolver: (Expression) => Expression): GroupWindow = this

  def validate(tableEnv: TableEnvironment): ValidationResult = alias match {
    case Some(WindowReference(_)) => ValidationSuccess
    case Some(_) => ValidationFailure("Window reference for group window expected.")
    case None => ValidationSuccess
  }

  override def toString: String = getClass.getSimpleName
}

abstract class EventTimeGroupWindow(
    name: Option[Expression],
    time: Expression,
    lateness: Option[Expression])
  extends GroupWindow(name) {

  override def validate(tableEnv: TableEnvironment): ValidationResult = {
    val valid = super.validate(tableEnv)
    if (valid.isFailure) {
        return valid
    }

    if (tableEnv.isInstanceOf[StreamTableEnvironment]) {
      val valid = time match {
        case RowtimeAttribute() | SystemtimeAttribute() =>
          ValidationSuccess
        case _ =>
          ValidationFailure("Event-time window expects a time field of either " +
            "'rowtime' or 'systemtime'.")
      }
      if (valid.isFailure) {
        return valid
      }
    }
    if (!TypeCoercion.canCast(time.resultType, BasicTypeInfo.LONG_TYPE_INFO)) {
      return ValidationFailure(s"Event-time window expects a time field that can be safely cast " +
        s"to Long, but is ${time.resultType}")
    }

    lateness match {
      case None =>
        ValidationSuccess
      case Some(Literal(_, IntervalTypeInfo.INTERVAL_MILLIS)) =>
        ValidationSuccess
      case _ =>
        ValidationFailure(
          "Event-time window expects allowed lateness literal of type Interval of Milliseconds.")
    }
  }
}

abstract class ProcessingTimeGroupWindow(name: Option[Expression]) extends GroupWindow(name)

// ------------------------------------------------------------------------------------------------

object TumblingGroupWindow {
  def validate(tableEnv: TableEnvironment, size: Expression): ValidationResult = size match {
    case Literal(_, IntervalTypeInfo.INTERVAL_MILLIS) =>
      ValidationSuccess
    case Literal(_, BasicTypeInfo.LONG_TYPE_INFO) | Literal(_, BasicTypeInfo.INT_TYPE_INFO) =>
      ValidationSuccess
    case _ =>
      ValidationFailure(
        "Tumbling window expects size literal of type Interval of Milliseconds or Long/Integer.")
  }
}

case class ProcessingTimeTumblingGroupWindow(
    name: Option[Expression],
    size: Expression)
  extends ProcessingTimeGroupWindow(name) {

  override def resolveExpressions(resolve: (Expression) => Expression): GroupWindow =
    ProcessingTimeTumblingGroupWindow(
      name.map(resolve),
      resolve(size))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(TumblingGroupWindow.validate(tableEnv, size))
}

case class EventTimeTumblingGroupWindow(
    name: Option[Expression],
    timeField: Expression,
    lateness: Option[Expression],
    size: Expression)
  extends EventTimeGroupWindow(
    name,
    timeField,
    lateness) {

  override def resolveExpressions(resolve: (Expression) => Expression): GroupWindow =
    EventTimeTumblingGroupWindow(
      name.map(resolve),
      resolve(timeField),
      lateness.map(resolve),
      resolve(size))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(TumblingGroupWindow.validate(tableEnv, size))
}

// ------------------------------------------------------------------------------------------------

object SlidingGroupWindow {
  def validate(
      tableEnv: TableEnvironment,
      size: Expression,
      slide: Expression)
    : ValidationResult = {

    val checkedSize = size match {
      case Literal(_, IntervalTypeInfo.INTERVAL_MILLIS) =>
        ValidationSuccess
      case Literal(_, BasicTypeInfo.INT_TYPE_INFO | BasicTypeInfo.LONG_TYPE_INFO) =>
        ValidationSuccess
      case _ =>
        ValidationFailure(
          "Sliding window expects size literal of type Interval of Milliseconds or Long/Integer.")
    }

    val checkedSlide = slide match {
      case Literal(_, IntervalTypeInfo.INTERVAL_MILLIS) =>
        ValidationSuccess
      case Literal(_, BasicTypeInfo.INT_TYPE_INFO | BasicTypeInfo.LONG_TYPE_INFO) =>
        ValidationSuccess
      case _ =>
        ValidationFailure(
          "Sliding window expects slide literal of type Interval of Milliseconds or Long/Integer.")
    }

    checkedSize
      .orElse(checkedSlide)
      .orElse {
        if (size.resultType != slide.resultType) {
          ValidationFailure("Sliding window expects same type of size and slide.")
        } else {
          ValidationSuccess
        }
      }
  }
}

case class ProcessingTimeSlidingGroupWindow(
    name: Option[Expression],
    size: Expression,
    slide: Expression)
  extends ProcessingTimeGroupWindow(name) {

  override def resolveExpressions(resolve: (Expression) => Expression): GroupWindow =
    ProcessingTimeSlidingGroupWindow(
      name.map(resolve),
      resolve(size),
      resolve(slide))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(SlidingGroupWindow.validate(tableEnv, size, slide))
}

case class EventTimeSlidingGroupWindow(
    name: Option[Expression],
    timeField: Expression,
    lateness: Option[Expression],
    size: Expression,
    slide: Expression)
  extends EventTimeGroupWindow(name, timeField, lateness) {

  override def resolveExpressions(resolve: (Expression) => Expression): GroupWindow =
    EventTimeSlidingGroupWindow(
      name.map(resolve),
      resolve(timeField),
      lateness.map(resolve),
      resolve(size),
      resolve(slide))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(SlidingGroupWindow.validate(tableEnv, size, slide))
}

// ------------------------------------------------------------------------------------------------

object SessionGroupWindow {

  def validate(tableEnv: TableEnvironment, gap: Expression): ValidationResult = gap match {
    case Literal(timeInterval: Long, IntervalTypeInfo.INTERVAL_MILLIS) =>
      ValidationSuccess
    case _ =>
      ValidationFailure(
        "Session window expects gap literal of type Interval of Milliseconds.")
  }
}

case class ProcessingTimeSessionGroupWindow(
    name: Option[Expression],
    gap: Expression)
  extends ProcessingTimeGroupWindow(name) {

  override def resolveExpressions(resolve: (Expression) => Expression): GroupWindow =
    ProcessingTimeSessionGroupWindow(
      name.map(resolve),
      resolve(gap))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(SessionGroupWindow.validate(tableEnv, gap))
}

case class EventTimeSessionGroupWindow(
    name: Option[Expression],
    timeField: Expression,
    lateness: Option[Expression],
    gap: Expression)
  extends EventTimeGroupWindow(
    name,
    timeField,
    lateness) {

  override def resolveExpressions(resolve: (Expression) => Expression): GroupWindow =
    EventTimeSessionGroupWindow(
      name.map(resolve),
      resolve(timeField),
      lateness.map(resolve),
      resolve(gap))

  override def validate(tableEnv: TableEnvironment): ValidationResult =
    super.validate(tableEnv).orElse(SessionGroupWindow.validate(tableEnv, gap))
}
