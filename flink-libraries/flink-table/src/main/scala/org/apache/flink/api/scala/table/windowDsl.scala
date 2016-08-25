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

package org.apache.flink.api.scala.table

import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.api.table.plan.logical._

trait Window {

  /**
    * Converts API class to logical GroupWindow for planning.
    */
  private[flink] def toGroupWindow: GroupWindow
}

abstract class EventTimeWindow(timeField: Expression) extends Window {

  protected var lateness: Option[Expression] = None

  protected var name: Option[Expression] = None

  def allowLateness(lateness: Expression): EventTimeWindow = {
    this.lateness = Some(lateness)
    this
  }

  def as(alias: Expression): EventTimeWindow = {
    this.name = Some(alias)
    this
  }
}

// ------------------------------------------------------------------------------------------------

object Tumble {
  def over(size: Expression): TumblingWindow = new TumblingWindow(size)
}

class TumblingWindow(size: Expression) extends Window {

  private var alias: Option[Expression] = None

  def on(timeField: Expression): TumblingEventTimeWindow =
    new TumblingEventTimeWindow(alias, timeField, size)

  def as(alias: Expression): TumblingWindow = {
    this.alias = Some(alias)
    this
  }

  override private[flink] def toGroupWindow: GroupWindow =
    ProcessingTimeTumblingGroupWindow(alias, size)
}

class TumblingEventTimeWindow(
    alias: Option[Expression],
    time: Expression,
    size: Expression)
  extends EventTimeWindow(time) {

  override private[flink] def toGroupWindow: GroupWindow =
    EventTimeTumblingGroupWindow(name.orElse(alias), time, lateness, size)
}

// ------------------------------------------------------------------------------------------------

object Slide {
  def over(size: Expression): SlideWithSize = new SlideWithSize(size)
}

class SlideWithSize(size: Expression) {
  def every(slide: Expression): SlidingWindow = new SlidingWindow(size, slide)
}

class SlidingWindow(
    size: Expression,
    slide: Expression)
  extends Window {

  private var alias: Option[Expression] = None

  def on(timeField: Expression): SlidingEventTimeWindow =
    new SlidingEventTimeWindow(alias, timeField, size, slide)

  def as(alias: Expression): SlidingWindow = {
    this.alias = Some(alias)
    this
  }

  override private[flink] def toGroupWindow: GroupWindow =
    ProcessingTimeSlidingGroupWindow(alias, size, slide)
}

class SlidingEventTimeWindow(
    alias: Option[Expression],
    timeField: Expression,
    size: Expression,
    slide: Expression)
  extends EventTimeWindow(timeField) {

  override private[flink] def toGroupWindow: GroupWindow =
    EventTimeSlidingGroupWindow(name.orElse(alias), timeField, lateness, size, slide)
}

// ------------------------------------------------------------------------------------------------

object Session {
  def withGap(size: Expression): SessionWindow = new SessionWindow(size)
}

class SessionWindow(gap: Expression) extends Window {

  private var alias: Option[Expression] = None

  def on(timeField: Expression): SessionEventTimeWindow =
    new SessionEventTimeWindow(alias, timeField, gap)

  def as(alias: Expression): SessionWindow = {
    this.alias = Some(alias)
    this
  }

  override private[flink] def toGroupWindow: GroupWindow =
    ProcessingTimeSessionGroupWindow(alias, gap)
}

class SessionEventTimeWindow(
    alias: Option[Expression],
    timeField: Expression,
    gap: Expression)
  extends EventTimeWindow(timeField) {

  override private[flink] def toGroupWindow: GroupWindow =
    EventTimeSessionGroupWindow(name.orElse(alias), timeField, lateness, gap)
}
