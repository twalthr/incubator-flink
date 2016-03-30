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

/**
  * General expression class to represent symbols.
  */
case class SymbolExpression(symbols: Symbols, symbolName: String) extends Expression {

  override def typeInfo: TypeInformation[_] = ???

  override def children: Seq[Expression] = Seq()

  def asExpression: Expression = this
}

/**
  * Enumeration for symbols.
  */
abstract class Symbols extends Enumeration {

  implicit def valueToSymbolExpression(value: Value): SymbolExpression =
    SymbolExpression(this, value.toString)

}

/**
  * Units for working with date, time, and date/time ranges.
  */
object DateTimeUnit extends Symbols {
  /**
    * This symbols need to be kept in sync
    * with [[org.apache.calcite.avatica.util.TimeUnitRange]].
    */
  val YEAR,
    YEAR_TO_MONTH,
    MONTH,
    DAY,
    DAY_TO_HOUR,
    DAY_TO_MINUTE,
    DAY_TO_SECOND,
    HOUR,
    HOUR_TO_MINUTE,
    HOUR_TO_SECOND,
    MINUTE,
    MINUTE_TO_SECOND,
    SECOND = Value
}

/**
  * Trim types for TRIM operation.
  */
object TrimType extends Symbols {
  /**
    * This symbols need to be kept in sync
    * with [[org.apache.calcite.sql.fun.SqlTrimFunction.Flag]].
    */
  val BOTH,
    LEADING,
    TRAILING = Value
}

