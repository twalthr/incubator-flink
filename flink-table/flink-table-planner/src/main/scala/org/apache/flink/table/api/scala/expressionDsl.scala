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
package org.apache.flink.table.api.scala

import java.lang.{Boolean => JBoolean, Byte => JByte, Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong, Short => JShort}
import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Time, Timestamp}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.expressions.ApiExpressionUtils._
import org.apache.flink.table.expressions.BuiltInFunctionDefinitions.{E => FDE, UUID => FDUUID, _}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{getAccumulatorTypeOfAggregateFunction, getResultTypeOfAggregateFunction}
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}

import _root_.scala.language.implicitConversions

/**
  * These are all the operations that can be used to construct an [[Expression]] AST for
  * expression operations.
  *
  * These operations must be kept in sync with the parser in
  * [[org.apache.flink.table.expressions.ExpressionParser]].
  */
trait ImplicitExpressionOperations extends BaseExpressionOperations[Expression] {
  private[flink] def expr: Expression

  /**
    * Enables literals on left side of binary expressions.
    *
    * e.g. 12.toExpr % 'a
    *
    * @return expression
    */
  def toExpr: Expression = expr

  /**
    * Specifies a name for an expression i.e. a field.
    *
    * @param name name for one field
    * @param extraNames additional names if the expression expands to multiple fields
    * @return field with an alias
    */
  def as(name: Symbol, extraNames: Symbol*): Expression =
    call(AS, toExpr +: valueLiteral(name.name) +: extraNames.map(name => valueLiteral(name.name)): _*)

  /**
    * Boolean AND in three-valued logic.
    */
  def && (other: Expression): Expression = call(AND, expr, other)

  /**
    * Boolean OR in three-valued logic.
    */
  def || (other: Expression): Expression = call(OR, expr, other)

  /**
    * Greater than.
    */
  def > (other: Expression): Expression = call(GREATER_THAN, expr, other)

  /**
    * Greater than or equal.
    */
  def >= (other: Expression): Expression = call(GREATER_THAN_OR_EQUAL, expr, other)

  /**
    * Less than.
    */
  def < (other: Expression): Expression = call(LESS_THAN, expr, other)

  /**
    * Less than or equal.
    */
  def <= (other: Expression): Expression = call(LESS_THAN_OR_EQUAL, expr, other)

  /**
    * Equals.
    */
  def === (other: Expression): Expression = call(EQUALS, expr, other)

  /**
    * Not equal.
    */
  def !== (other: Expression): Expression = call(NOT_EQUALS, expr, other)

  /**
    * Whether boolean expression is not true; returns null if boolean is null.
    */
  def unary_! : Expression = call(NOT, expr)

  /**
    * Returns negative numeric.
    */
  def unary_- : Expression = call(MINUS_PREFIX, expr)

  /**
    * Returns numeric.
    */
  def unary_+ : Expression = expr

  /**
    * Returns left plus right.
    */
  def + (other: Expression): Expression = call(PLUS, expr, other)

  /**
    * Returns left minus right.
    */
  def - (other: Expression): Expression = call(MINUS, expr, other)

  /**
    * Returns left divided by right.
    */
  def / (other: Expression): Expression = call(DIVIDE, expr, other)

  /**
    * Returns left multiplied by right.
    */
  def * (other: Expression): Expression = call(TIMES, expr, other)

  /**
    * Returns the remainder (modulus) of left divided by right.
    * The result is negative only if left is negative.
    */
  def % (other: Expression): Expression = mod(other)

  /**
    * Ternary conditional operator that decides which of two other expressions should be
    * based on a evaluated boolean condition.
    *
    * e.g. (42 > 5).?("A", "B") leads to "A"
    *
    * @param ifTrue expression to be evaluated if condition holds
    * @param ifFalse expression to be evaluated if condition does not hold
    */
  def ?(ifTrue: Expression, ifFalse: Expression): Expression = call(IF, expr, ifTrue, ifFalse)  // Interval types

  /**
    * Creates an interval of the given number of years.
    *
    * @return interval of months
    */
  def year: Expression = toMonthInterval(expr, 12)

  /**
    * Creates an interval of the given number of years.
    *
    * @return interval of months
    */
  def years: Expression = year

  /**
    * Creates an interval of the given number of quarters.
    *
    * @return interval of months
    */
  def quarter: Expression = toMonthInterval(expr, 3)

  /**
    * Creates an interval of the given number of quarters.
    *
    * @return interval of months
    */
  def quarters: Expression = quarter

  /**
    * Creates an interval of the given number of months.
    *
    * @return interval of months
    */
  def month: Expression = toMonthInterval(expr, 1)

  /**
    * Creates an interval of the given number of months.
    *
    * @return interval of months
    */
  def months: Expression = month

  /**
    * Creates an interval of the given number of weeks.
    *
    * @return interval of milliseconds
    */
  def week: Expression = toMilliInterval(expr, 7 * MILLIS_PER_DAY)

  /**
    * Creates an interval of the given number of weeks.
    *
    * @return interval of milliseconds
    */
  def weeks: Expression = week

  /**
    * Creates an interval of the given number of days.
    *
    * @return interval of milliseconds
    */
  def day: Expression = toMilliInterval(expr, MILLIS_PER_DAY)

  /**
    * Creates an interval of the given number of days.
    *
    * @return interval of milliseconds
    */
  def days: Expression = day

  /**
    * Creates an interval of the given number of hours.
    *
    * @return interval of milliseconds
    */
  def hour: Expression = toMilliInterval(expr, MILLIS_PER_HOUR)

  /**
    * Creates an interval of the given number of hours.
    *
    * @return interval of milliseconds
    */
  def hours: Expression = hour

  /**
    * Creates an interval of the given number of minutes.
    *
    * @return interval of milliseconds
    */
  def minute: Expression = toMilliInterval(expr, MILLIS_PER_MINUTE)

  /**
    * Creates an interval of the given number of minutes.
    *
    * @return interval of milliseconds
    */
  def minutes: Expression = minute

  /**
    * Creates an interval of the given number of seconds.
    *
    * @return interval of milliseconds
    */
  def second: Expression = toMilliInterval(expr, MILLIS_PER_SECOND)

  /**
    * Creates an interval of the given number of seconds.
    *
    * @return interval of milliseconds
    */
  def seconds: Expression = second

  /**
    * Creates an interval of the given number of milliseconds.
    *
    * @return interval of milliseconds
    */
  def milli: Expression = toMilliInterval(expr, 1)

  /**
    * Creates an interval of the given number of milliseconds.
    *
    * @return interval of milliseconds
    */
  def millis: Expression = milli

  // Row interval type

  /**
    * Creates an interval of rows.
    *
    * @return interval of rows
    */
  def rows: Expression = toRowInterval(expr)
}

/**
  * Implicit conversions from Scala literals to [[Expression]] and from [[Expression]]
  * to [[ImplicitExpressionOperations]].
  */
trait ImplicitExpressionConversions {

  /**
    * Offset constant to be used in the `preceding` clause of unbounded [[Over]] windows. Use this
    * constant for a time interval. Unbounded over windows start with the first row of a partition.
    */
  implicit val UNBOUNDED_ROW: Expression = call(BuiltInFunctionDefinitions.UNBOUNDED_ROW)

  /**
    * Offset constant to be used in the `preceding` clause of unbounded [[Over]] windows. Use this
    * constant for a row-count interval. Unbounded over windows start with the first row of a
    * partition.
    */
  implicit val UNBOUNDED_RANGE: Expression = call(BuiltInFunctionDefinitions.UNBOUNDED_RANGE)

  /**
    * Offset constant to be used in the `following` clause of [[Over]] windows. Use this for setting
    * the upper bound of the window to the current row.
    */
  implicit val CURRENT_ROW: Expression = call(BuiltInFunctionDefinitions.CURRENT_ROW)

  /**
    * Offset constant to be used in the `following` clause of [[Over]] windows. Use this for setting
    * the upper bound of the window to the sort key of the current row, i.e., all rows with the same
    * sort key as the current row are included in the window.
    */
  implicit val CURRENT_RANGE: Expression = call(BuiltInFunctionDefinitions.CURRENT_RANGE)

  implicit class WithOperations(e: Expression) extends ImplicitExpressionOperations {
    def expr: Expression = e
  }

  implicit class UnresolvedFieldExpression(s: Symbol) extends ImplicitExpressionOperations {
    def expr: Expression = fieldRef(s.name)
  }

  implicit class LiteralLongExpression(l: Long) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(l)
  }

  implicit class LiteralByteExpression(b: Byte) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(b)
  }

  implicit class LiteralShortExpression(s: Short) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(s)
  }

  implicit class LiteralIntExpression(i: Int) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(i)
  }

  implicit class LiteralFloatExpression(f: Float) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(f)
  }

  implicit class LiteralDoubleExpression(d: Double) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(d)
  }

  implicit class LiteralStringExpression(str: String) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(str)
  }

  implicit class LiteralBooleanExpression(bool: Boolean) extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(bool)
  }

  implicit class LiteralJavaDecimalExpression(javaDecimal: JBigDecimal)
      extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(javaDecimal)
  }

  implicit class LiteralScalaDecimalExpression(scalaDecimal: BigDecimal)
      extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(scalaDecimal.bigDecimal)
  }

  implicit class LiteralSqlDateExpression(sqlDate: Date)
      extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(sqlDate)
  }

  implicit class LiteralSqlTimeExpression(sqlTime: Time)
    extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(sqlTime)
  }

  implicit class LiteralSqlTimestampExpression(sqlTimestamp: Timestamp)
      extends ImplicitExpressionOperations {
    def expr: Expression = valueLiteral(sqlTimestamp)
  }

  implicit class ScalarFunctionCall(val s: ScalarFunction) {

    /**
      * Calls a scalar function for the given parameters.
      */
    def apply(params: Expression*): Expression = {
      call(new ScalarFunctionDefinition(s.getClass.getName, s), params:_*)
    }
  }

  implicit class TableFunctionCall[T: TypeInformation](val t: TableFunction[T]) {

    /**
      * Calls a table function for the given parameters.
      */
    def apply(params: Expression*): Expression = {
      val resultType = if (t.getResultType == null) {
        implicitly[TypeInformation[T]]
      } else {
        t.getResultType
      }
      call(new TableFunctionDefinition(t.getClass.getName, t, resultType), params: _*)
    }
  }

  implicit class AggregateFunctionCall[T: TypeInformation, ACC: TypeInformation]
      (val a: AggregateFunction[T, ACC]) {

    private def createFunctionDefinition(): AggregateFunctionDefinition = {
      val resultTypeInfo: TypeInformation[_] = getResultTypeOfAggregateFunction(
        a,
        implicitly[TypeInformation[T]])

      val accTypeInfo: TypeInformation[_] = getAccumulatorTypeOfAggregateFunction(
        a,
        implicitly[TypeInformation[ACC]])

      new AggregateFunctionDefinition(a.getClass.getName, a, resultTypeInfo, accTypeInfo)
    }

    /**
      * Calls an aggregate function for the given parameters.
      */
    def apply(params: Expression*): Expression = {
      call(createFunctionDefinition(), params: _*)
    }

    /**
      * Calculates the aggregate results only for distinct values.
      */
    def distinct(params: Expression*): Expression = {
      call(DISTINCT, apply(params: _*))
    }
  }

  implicit def tableSymbolToExpression(sym: TableSymbol): Expression =
    symbol(sym)

  implicit def symbol2FieldExpression(sym: Symbol): Expression =
    fieldRef(sym.name)

  implicit def byte2Literal(b: Byte): Expression = valueLiteral(b)

  implicit def short2Literal(s: Short): Expression = valueLiteral(s)

  implicit def int2Literal(i: Int): Expression = valueLiteral(i)

  implicit def long2Literal(l: Long): Expression = valueLiteral(l)

  implicit def double2Literal(d: Double): Expression = valueLiteral(d)

  implicit def float2Literal(d: Float): Expression = valueLiteral(d)

  implicit def string2Literal(str: String): Expression = valueLiteral(str)

  implicit def boolean2Literal(bool: Boolean): Expression = valueLiteral(bool)

  implicit def javaDec2Literal(javaDec: JBigDecimal): Expression = valueLiteral(javaDec)

  implicit def scalaDec2Literal(scalaDec: BigDecimal): Expression =
    valueLiteral(scalaDec.bigDecimal)

  implicit def sqlDate2Literal(sqlDate: Date): Expression = valueLiteral(sqlDate)

  implicit def sqlTime2Literal(sqlTime: Time): Expression = valueLiteral(sqlTime)

  implicit def sqlTimestamp2Literal(sqlTimestamp: Timestamp): Expression =
    valueLiteral(sqlTimestamp)

  implicit def array2ArrayConstructor(array: Array[_]): Expression = {

    def createArray(elements: Array[_]): Expression = {
      call(BuiltInFunctionDefinitions.ARRAY, elements.map(valueLiteral): _*)
    }

    def convertArray(array: Array[_]): Expression = array match {
      // primitives
      case _: Array[Boolean] => createArray(array)
      case _: Array[Byte] => createArray(array)
      case _: Array[Short] => createArray(array)
      case _: Array[Int] => createArray(array)
      case _: Array[Long] => createArray(array)
      case _: Array[Float] => createArray(array)
      case _: Array[Double] => createArray(array)

      // boxed types
      case _: Array[JBoolean] => createArray(array)
      case _: Array[JByte] => createArray(array)
      case _: Array[JShort] => createArray(array)
      case _: Array[JInteger] => createArray(array)
      case _: Array[JLong] => createArray(array)
      case _: Array[JFloat] => createArray(array)
      case _: Array[JDouble] => createArray(array)

      // others
      case _: Array[String] => createArray(array)
      case _: Array[JBigDecimal] => createArray(array)
      case _: Array[Date] => createArray(array)
      case _: Array[Time] => createArray(array)
      case _: Array[Timestamp] => createArray(array)
      case bda: Array[BigDecimal] => createArray(bda.map(_.bigDecimal))

      case _ =>
        // nested
        if (array.length > 0 && array.head.isInstanceOf[Array[_]]) {
          call(
            BuiltInFunctionDefinitions.ARRAY,
            array.map { na => convertArray(na.asInstanceOf[Array[_]]) } :_*)
        } else {
          throw new ValidationException("Unsupported array type.")
        }
    }

    convertArray(array)
  }
}

// ------------------------------------------------------------------------------------------------
// Expressions with no parameters
// ------------------------------------------------------------------------------------------------

// we disable the object checker here as it checks for capital letters of objects
// but we want that objects look like functions in certain cases e.g. array(1, 2, 3)
// scalastyle:off object.name

/**
  * Returns the current SQL date in UTC time zone.
  */
object currentDate {

  /**
    * Returns the current SQL date in UTC time zone.
    */
  def apply(): Expression = {
    call(CURRENT_DATE)
  }
}

/**
  * Returns the current SQL time in UTC time zone.
  */
object currentTime {

  /**
    * Returns the current SQL time in UTC time zone.
    */
  def apply(): Expression = {
    call(CURRENT_TIME)
  }
}

/**
  * Returns the current SQL timestamp in UTC time zone.
  */
object currentTimestamp {

  /**
    * Returns the current SQL timestamp in UTC time zone.
    */
  def apply(): Expression = {
    call(CURRENT_TIMESTAMP)
  }
}

/**
  * Returns the current SQL time in local time zone.
  */
object localTime {

  /**
    * Returns the current SQL time in local time zone.
    */
  def apply(): Expression = {
    call(LOCAL_TIME)
  }
}

/**
  * Returns the current SQL timestamp in local time zone.
  */
object localTimestamp {

  /**
    * Returns the current SQL timestamp in local time zone.
    */
  def apply(): Expression = {
    call(LOCAL_TIMESTAMP)
  }
}

/**
  * Determines whether two anchored time intervals overlap. Time point and temporal are
  * transformed into a range defined by two time points (start, end). The function
  * evaluates <code>leftEnd >= rightStart && rightEnd >= leftStart</code>.
  *
  * It evaluates: leftEnd >= rightStart && rightEnd >= leftStart
  *
  * e.g. temporalOverlaps("2:55:00".toTime, 1.hour, "3:30:00".toTime, 2.hour) leads to true
  */
object temporalOverlaps {

  /**
    * Determines whether two anchored time intervals overlap. Time point and temporal are
    * transformed into a range defined by two time points (start, end).
    *
    * It evaluates: leftEnd >= rightStart && rightEnd >= leftStart
    *
    * e.g. temporalOverlaps("2:55:00".toTime, 1.hour, "3:30:00".toTime, 2.hour) leads to true
    */
  def apply(
      leftTimePoint: Expression,
      leftTemporal: Expression,
      rightTimePoint: Expression,
      rightTemporal: Expression)
    : Expression = {
    call(TEMPORAL_OVERLAPS, leftTimePoint, leftTemporal, rightTimePoint, rightTemporal)
  }
}

/**
  * Formats a timestamp as a string using a specified format.
  * The format must be compatible with MySQL's date formatting syntax as used by the
  * date_parse function.
  *
  * For example <code>dataFormat('time, "%Y, %d %M")</code> results in strings
  * formatted as "2017, 05 May".
  */
object dateFormat {

  /**
    * Formats a timestamp as a string using a specified format.
    * The format must be compatible with MySQL's date formatting syntax as used by the
    * date_parse function.
    *
    * For example dataFormat('time, "%Y, %d %M") results in strings formatted as "2017, 05 May".
    *
    * @param timestamp The timestamp to format as string.
    * @param format The format of the string.
    * @return The formatted timestamp as string.
    */
  def apply(
      timestamp: Expression,
      format: Expression)
    : Expression = {
    call(DATE_FORMAT, timestamp, format)
  }
}

/**
  * Returns the (signed) number of [[TimePointUnit]] between timePoint1 and timePoint2.
  *
  * For example, timestampDiff(TimePointUnit.DAY, '2016-06-15'.toDate, '2016-06-18'.toDate leads
  * to 3.
  */
object timestampDiff {

  /**
    * Returns the (signed) number of [[TimePointUnit]] between timePoint1 and timePoint2.
    *
    * For example, timestampDiff(TimePointUnit.DAY, '2016-06-15'.toDate, '2016-06-18'.toDate leads
    * to 3.
    *
    * @param timePointUnit The unit to compute diff.
    * @param timePoint1 The first point in time.
    * @param timePoint2 The second point in time.
    * @return The number of intervals as integer value.
    */
  def apply(
      timePointUnit: TimePointUnit,
      timePoint1: Expression,
      timePoint2: Expression)
    : Expression = {
    call(TIMESTAMP_DIFF, timePointUnit, timePoint1, timePoint2)
  }
}

/**
  * Creates an array of literals. The array will be an array of objects (not primitives).
  */
object array {

  /**
    * Creates an array of literals. The array will be an array of objects (not primitives).
    */
  def apply(head: Expression, tail: Expression*): Expression = {
    call(ARRAY, head +: tail: _*)
  }
}

/**
  * Creates a row of expressions.
  */
object row {

  /**
    * Creates a row of expressions.
    */
  def apply(head: Expression, tail: Expression*): Expression = {
    call(ROW, head +: tail: _*)
  }
}

/**
  * Creates a map of expressions. The map will be a map between two objects (not primitives).
  */
object map {

  /**
    * Creates a map of expressions. The map will be a map between two objects (not primitives).
    */
  def apply(key: Expression, value: Expression, tail: Expression*): Expression = {
    call(MAP, key +: value +: tail: _*)
  }
}

/**
  * Returns a value that is closer than any other value to pi.
  */
object pi {

  /**
    * Returns a value that is closer than any other value to pi.
    */
  def apply(): Expression = {
    call(PI)
  }
}

/**
  * Returns a value that is closer than any other value to e.
  */
object e {

  /**
    * Returns a value that is closer than any other value to e.
    */
  def apply(): Expression = {
    call(FDE)
  }
}

/**
  * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive).
  */
object rand {

  /**
    * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive).
    */
  def apply(): Expression = {
    call(RAND)
  }

  /**
    * Returns a pseudorandom double value between 0.0 (inclusive) and 1.0 (exclusive) with a
    * initial seed. Two rand() functions will return identical sequences of numbers if they
    * have same initial seed.
    */
  def apply(seed: Expression): Expression = {
    call(RAND, seed)
  }
}

/**
  * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified
  * value (exclusive).
  */
object randInteger {

  /**
    * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified
    * value (exclusive).
    */
  def apply(bound: Expression): Expression = {
    call(RAND_INTEGER, bound)
  }

  /**
    * Returns a pseudorandom integer value between 0.0 (inclusive) and the specified value
    * (exclusive) with a initial seed. Two randInteger() functions will return identical sequences
    * of numbers if they have same initial seed and same bound.
    */
  def apply(seed: Expression, bound: Expression): Expression = {
    call(RAND_INTEGER, seed, bound)
  }
}

/**
  * Returns the string that results from concatenating the arguments.
  * Returns NULL if any argument is NULL.
  */
object concat {

  /**
    * Returns the string that results from concatenating the arguments.
    * Returns NULL if any argument is NULL.
    */
  def apply(string: Expression, strings: Expression*): Expression = {
    call(CONCAT, string +: strings: _*)
  }
}

/**
  * Calculates the arc tangent of a given coordinate.
  */
object atan2 {

  /**
    * Calculates the arc tangent of a given coordinate.
    */
  def apply(y: Expression, x: Expression): Expression = {
    call(ATAN2, y, x)
  }
}

/**
  * Returns the string that results from concatenating the arguments and separator.
  * Returns NULL If the separator is NULL.
  *
  * Note: this user-defined function does not skip empty strings. However, it does skip any NULL
  * values after the separator argument.
  **/
object concat_ws {
  def apply(separator: Expression, string: Expression, strings: Expression*): Expression = {
    call(CONCAT_WS, separator +: string +: strings: _*)
  }
}

/**
  * Returns an UUID (Universally Unique Identifier) string (e.g.,
  * "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly
  * generated) UUID. The UUID is generated using a cryptographically strong pseudo random number
  * generator.
  */
object uuid {

  /**
    * Returns an UUID (Universally Unique Identifier) string (e.g.,
    * "3d3c68f7-f608-473f-b60c-b0c44ad4cc4e") according to RFC 4122 type 4 (pseudo randomly
    * generated) UUID. The UUID is generated using a cryptographically strong pseudo random number
    * generator.
    */
  def apply(): Expression = {
    call(FDUUID)
  }
}

/**
  * Returns a null literal value of a given type.
  *
  * e.g. nullOf(Types.INT)
  */
object nullOf {

  /**
    * Returns a null literal value of a given type.
    *
    * e.g. nullOf(Types.INT)
    */
  def apply(typeInfo: TypeInformation[_]): Expression = {
    valueLiteral(null, typeInfo)
  }
}

/**
  * Calculates the logarithm of the given value.
  */
object log {

  /**
    * Calculates the natural logarithm of the given value.
    */
  def apply(value: Expression): Expression = {
    call(LOG, value)
  }

  /**
    * Calculates the logarithm of the given value to the given base.
    */
  def apply(base: Expression, value: Expression): Expression = {
    call(LOG, base, value)
  }
}

/**
  * Ternary conditional operator that decides which of two other expressions should be evaluated
  * based on a evaluated boolean condition.
  *
  * e.g. ifThenElse(42 > 5, "A", "B") leads to "A"
  */
object ifThenElse {

  /**
    * Ternary conditional operator that decides which of two other expressions should be evaluated
    * based on a evaluated boolean condition.
    *
    * e.g. ifThenElse(42 > 5, "A", "B") leads to "A"
    *
    * @param condition boolean condition
    * @param ifTrue expression to be evaluated if condition holds
    * @param ifFalse expression to be evaluated if condition does not hold
    */
  def apply(condition: Expression, ifTrue: Expression, ifFalse: Expression): Expression = {
    call(IF, condition, ifTrue, ifFalse)
  }
}

// scalastyle:on object.name
