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

package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.{SqlTimeTypeInfo, TypeInformation}
import org.apache.flink.table.api.Table
import org.apache.flink.table.expressions.ApiExpressionUtils._
import org.apache.flink.table.expressions.BuiltInFunctionDefinitions._

abstract class BaseExpressionOperations[T >: Expression] {

  def toExpr: Expression

  def isEqualTo(other: Expression): T = {
    call(EQUALS, toExpr, other)
  }

  def isGreaterThan(other: Expression): T = {
    call(GREATER_THAN, toExpr, other)
  }

  def isGreaterThanOrEqualTo(other: Expression): T = {
    call(GREATER_THAN_OR_EQUAL, toExpr, other)
  }

  def isLessThan(other: Expression): T = {
    call(LESS_THAN, toExpr, other)
  }

  def isLessThanOrEqualTo(other: Expression): T = {
    call(LESS_THAN_OR_EQUAL, toExpr, other)
  }

  def isNotEqualTo(other: Expression): T = {
    call(NOT_EQUALS, toExpr, other)
  }

  def minus(other: Expression): T = {
    call(MINUS, toExpr, other)
  }

  def plus(other: Expression): T = {
    call(PLUS, toExpr, other)
  }

  def dividedBy(other: Expression): T = {
    call(DIVIDE, toExpr, other)
  }

  def multipliedBy(other: Expression): T = {
    call(TIMES, toExpr, other)
  }

  def modulo(other: Expression): T = {
    call(MOD, toExpr, other)
  }

  /**
    * Returns true if the given expression is between lowerBound and upperBound (both inclusive).
    * False otherwise. The parameters must be numeric types or identical comparable types.
    *
    * @param lowerBound numeric or comparable expression
    * @param upperBound numeric or comparable expression
    * @return boolean or null
    */
  def between(lowerBound: Expression, upperBound: Expression): T =
    call(BETWEEN, toExpr, lowerBound, upperBound)

  /**
    * Returns true if the given expression is not between lowerBound and upperBound (both
    * inclusive). False otherwise. The parameters must be numeric types or identical
    * comparable types.
    *
    * @param lowerBound numeric or comparable expression
    * @param upperBound numeric or comparable expression
    * @return boolean or null
    */
  def notBetween(lowerBound: Expression, upperBound: Expression): T =
    call(NOT_BETWEEN, toExpr, lowerBound, upperBound)
  /**
    * Returns true if the given expression is null.
    */
  def isNull: T = call(IS_NULL, toExpr)

  /**
    * Returns true if the given expression is not null.
    */
  def isNotNull: T = call(IS_NOT_NULL, toExpr)

  /**
    * Returns true if given boolean expression is true. False otherwise (for null and false).
    */
  def isTrue: T = call(IS_TRUE, toExpr)

  /**
    * Returns true if given boolean expression is false. False otherwise (for null and true).
    */
  def isFalse: T = call(IS_FALSE, toExpr)

  /**
    * Returns true if given boolean expression is not true (for null and false). False otherwise.
    */
  def isNotTrue: T = call(IS_NOT_TRUE, toExpr)

  /**
    * Returns true if given boolean expression is not false (for null and true). False otherwise.
    */
  def isNotFalse: T = call(IS_NOT_FALSE, toExpr)

  /**
    * Similar to a SQL distinct aggregation clause such as COUNT(DISTINCT a), declares that an
    * aggregation function is only applied on distinct input values.
    *
    * For example:
    *
    * {{{
    * orders
    *   .groupBy('a)
    *   .select('a, 'b.sum.distinct as 'd)
    * }}}
    */
  def distinct: T = call(DISTINCT, toExpr)

  /**
    * Returns the sum of the numeric field across all input values.
    * If all values are null, null is returned.
    */
  def sum: T = call(SUM, toExpr)

  /**
    * Returns the sum of the numeric field across all input values.
    * If all values are null, 0 is returned.
    */
  def sum0: T = call(SUM0, toExpr)

  /**
    * Returns the minimum value of field across all input values.
    */
  def min: T = call(MIN, toExpr)

  /**
    * Returns the maximum value of field across all input values.
    */
  def max: T = call(MAX, toExpr)

  /**
    * Returns the number of input rows for which the field is not null.
    */
  def count: T = call(COUNT, toExpr)

  /**
    * Returns the average (arithmetic mean) of the numeric field across all input values.
    */
  def avg: T = call(AVG, toExpr)

  /**
    * Returns the population standard deviation of an expression (the square root of varPop()).
    */
  def stddevPop: T = call(STDDEV_POP, toExpr)

  /**
    * Returns the sample standard deviation of an expression (the square root of varSamp()).
    */
  def stddevSamp: T = call(STDDEV_SAMP, toExpr)

  /**
    * Returns the population standard variance of an expression.
    */
  def varPop: T = call(VAR_POP, toExpr)

  /**
    *  Returns the sample variance of a given expression.
    */
  def varSamp: T = call(VAR_SAMP, toExpr)

  /**
    * Returns multiset aggregate of a given expression.
    */
  def collect: T = call(COLLECT, toExpr)

  /**
    * Converts a value to a given type.
    *
    * e.g. "42".cast(Types.INT) leads to 42.
    *
    * @return casted expression
    */
  def cast(toType: TypeInformation[_]): T =
    call(CAST, toExpr, typeLiteral(toType))

  /**
    * Specifies ascending order of an expression i.e. a field for orderBy call.
    *
    * @return ascend expression
    */
  def asc: T = call(ORDER_ASC, toExpr)

  /**
    * Specifies descending order of an expression i.e. a field for orderBy call.
    *
    * @return descend expression
    */
  def desc: T = call(ORDER_DESC, toExpr)

  /**
    * Returns true if an expression exists in a given list of expressions. This is a shorthand
    * for multiple OR conditions.
    *
    * If the testing set contains null, the result will be null if the element can not be found
    * and true if it can be found. If the element is null, the result is always null.
    *
    * e.g. "42".in(1, 2, 3) leads to false.
    */
  def in(elements: Expression*): T = call(IN, toExpr +: elements: _*)

  /**
    * Returns true if an expression exists in a given table sub-query. The sub-query table
    * must consist of one column. This column must have the same data type as the expression.
    *
    * Note: This operation is not supported in a streaming environment yet.
    */
  def in(table: Table): T = call(IN, toExpr, tableRef(table.toString, table))

  /**
    * Returns the start time (inclusive) of a window when applied on a window reference.
    */
  def start: T = call(WINDOW_START, toExpr)

  /**
    * Returns the end time (exclusive) of a window when applied on a window reference.
    *
    * e.g. if a window ends at 10:59:59.999 this property will return 11:00:00.000.
    */
  def end: T = call(WINDOW_END, toExpr)

  // scalar functions

  /**
    * Calculates the remainder of division the given number by another one.
    */
  def mod(other: Expression): T = call(MOD, toExpr, other)

  /**
    * Calculates the Euler's number raised to the given power.
    */
  def exp(): T = call(EXP, toExpr)

  /**
    * Calculates the base 10 logarithm of the given value.
    */
  def log10(): T = call(LOG10, toExpr)

  /**
    * Calculates the base 2 logarithm of the given value.
    */
  def log2(): T = call(LOG2, toExpr)

  /**
    * Calculates the natural logarithm of the given value.
    */
  def ln(): T = call(LN, toExpr)

  /**
    * Calculates the natural logarithm of the given value.
    */
  def log(): T = call(LOG, toExpr)

  /**
    * Calculates the logarithm of the given value to the given base.
    */
  def log(base: Expression): T = call(LOG, base, toExpr)

  /**
    * Calculates the given number raised to the power of the other value.
    */
  def power(other: Expression): T = call(POWER, toExpr, other)

  /**
    * Calculates the hyperbolic cosine of a given value.
    */
  def cosh(): T = call(COSH, toExpr)

  /**
    * Calculates the square root of a given value.
    */
  def sqrt(): T = call(SQRT, toExpr)

  /**
    * Calculates the absolute value of given value.
    */
  def abs(): T = call(ABS, toExpr)

  /**
    * Calculates the largest integer less than or equal to a given number.
    */
  def floor(): T = call(FLOOR, toExpr)

  /**
    * Calculates the hyperbolic sine of a given value.
    */
  def sinh(): T = call(SINH, toExpr)

  /**
    * Calculates the smallest integer greater than or equal to a given number.
    */
  def ceil(): T = call(CEIL, toExpr)

  /**
    * Calculates the sine of a given number.
    */
  def sin(): T = call(SIN, toExpr)

  /**
    * Calculates the cosine of a given number.
    */
  def cos(): T = call(COS, toExpr)

  /**
    * Calculates the tangent of a given number.
    */
  def tan(): T = call(TAN, toExpr)

  /**
    * Calculates the cotangent of a given number.
    */
  def cot(): T = call(COT, toExpr)

  /**
    * Calculates the arc sine of a given number.
    */
  def asin(): T = call(ASIN, toExpr)

  /**
    * Calculates the arc cosine of a given number.
    */
  def acos(): T = call(ACOS, toExpr)

  /**
    * Calculates the arc tangent of a given number.
    */
  def atan(): T = call(ATAN, toExpr)

  /**
    * Calculates the hyperbolic tangent of a given number.
    */
  def tanh(): T = call(TANH, toExpr)

  /**
    * Converts numeric from radians to degrees.
    */
  def degrees(): T = call(DEGREES, toExpr)

  /**
    * Converts numeric from degrees to radians.
    */
  def radians(): T = call(RADIANS, toExpr)

  /**
    * Calculates the signum of a given number.
    */
  def sign(): T = call(SIGN, toExpr)

  /**
    * Rounds the given number to integer places right to the decimal point.
    */
  def round(places: Expression): T = call(ROUND, toExpr, places)

  /**
    * Returns a string representation of an integer numeric value in binary format. Returns null if
    * numeric is null. E.g. "4" leads to "100", "12" leads to "1100".
    */
  def bin(): T = call(BIN, toExpr)

  /**
    * Returns a string representation of an integer numeric value or a string in hex format. Returns
    * null if numeric or string is null.
    *
    * E.g. a numeric 20 leads to "14", a numeric 100 leads to "64", and a string "hello,world" leads
    * to "68656c6c6f2c776f726c64".
    */
  def hex(): T = call(HEX, toExpr)

  /**
    * Returns a number of truncated to n decimal places.
    * If n is 0,the result has no decimal point or fractional part.
    * n can be negative to cause n digits left of the decimal point of the value to become zero.
    * E.g. truncate(42.345, 2) to 42.34.
    */
  def truncate(n: Expression): T = call(TRUNCATE, toExpr, n)

  /**
    * Returns a number of truncated to 0 decimal places.
    * E.g. truncate(42.345) to 42.0.
    */
  def truncate(): T = call(TRUNCATE, toExpr)

  // String operations

  /**
    * Creates a substring of the given string at given index for a given length.
    *
    * @param beginIndex first character of the substring (starting at 1, inclusive)
    * @param length number of characters of the substring
    * @return substring
    */
  def substring(beginIndex: Expression, length: Expression): T =
    call(SUBSTRING, toExpr, beginIndex, length)

  /**
    * Creates a substring of the given string beginning at the given index to the end.
    *
    * @param beginIndex first character of the substring (starting at 1, inclusive)
    * @return substring
    */
  def substring(beginIndex: Expression): T =
    call(SUBSTRING, toExpr, beginIndex)

  /**
    * Removes leading and/or trailing characters from the given string.
    *
    * @param removeLeading if true, remove leading characters (default: true)
    * @param removeTrailing if true, remove trailing characters (default: true)
    * @param character string containing the character (default: " ")
    * @return trimmed string
    */
  def trim(
      removeLeading: Boolean = true,
      removeTrailing: Boolean = true,
      character: Expression = valueLiteral(" "))
    : T = {
    call(TRIM, valueLiteral(removeLeading), valueLiteral(removeTrailing), character, toExpr)
  }

  /**
    * Returns a new string which replaces all the occurrences of the search target
    * with the replacement string (non-overlapping).
    */
  def replace(search: Expression, replacement: Expression): T =
    call(REPLACE, toExpr, search, replacement)

  /**
    * Returns the length of a string.
    */
  def charLength(): T = call(CHAR_LENGTH, toExpr)

  /**
    * Returns all of the characters in a string in upper case using the rules of
    * the default locale.
    */
  def upperCase(): T = call(UPPER, toExpr)

  /**
    * Returns all of the characters in a string in lower case using the rules of
    * the default locale.
    */
  def lowerCase(): T = call(LOWER, toExpr)

  /**
    * Converts the initial letter of each word in a string to uppercase.
    * Assumes a string containing only [A-Za-z0-9], everything else is treated as whitespace.
    */
  def initCap(): T = call(INIT_CAP, toExpr)

  /**
    * Returns true, if a string matches the specified LIKE pattern.
    *
    * e.g. "Jo_n%" matches all strings that start with "Jo(arbitrary letter)n"
    */
  def like(pattern: Expression): T = call(LIKE, toExpr, pattern)

  /**
    * Returns true, if a string matches the specified SQL regex pattern.
    *
    * e.g. "A+" matches all strings that consist of at least one A
    */
  def similar(pattern: Expression): T = call(SIMILAR, toExpr, pattern)

  /**
    * Returns the position of string in an other string starting at 1.
    * Returns 0 if string could not be found.
    *
    * e.g. "a".position("bbbbba") leads to 6
    */
  def position(haystack: Expression): T = call(POSITION, toExpr, haystack)

  /**
    * Returns a string left-padded with the given pad string to a length of len characters. If
    * the string is longer than len, the return value is shortened to len characters.
    *
    * e.g. "hi".lpad(4, '??') returns "??hi",  "hi".lpad(1, '??') returns "h"
    */
  def lpad(len: Expression, pad: Expression): T = call(LPAD, toExpr, len, pad)

  /**
    * Returns a string right-padded with the given pad string to a length of len characters. If
    * the string is longer than len, the return value is shortened to len characters.
    *
    * e.g. "hi".rpad(4, '??') returns "hi??",  "hi".rpad(1, '??') returns "h"
    */
  def rpad(len: Expression, pad: Expression): T = call(RPAD, toExpr, len, pad)

  /**
    * Defines an aggregation to be used for a previously specified over window.
    *
    * For example:
    *
    * {{{
    * table
    *   .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
    *   .select('c, 'a, 'a.count over 'w, 'a.sum over 'w)
    * }}}
    */
  def over(alias: Expression): T = call(OVER, toExpr, alias)

  /**
    * Replaces a substring of string with a string starting at a position (starting at 1).
    *
    * e.g. "xxxxxtest".overlay("xxxx", 6) leads to "xxxxxxxxx"
    */
  def overlay(newString: Expression, starting: Expression): T =
    call(OVERLAY, toExpr, newString, starting)

  /**
    * Replaces a substring of string with a string starting at a position (starting at 1).
    * The length specifies how many characters should be removed.
    *
    * e.g. "xxxxxtest".overlay("xxxx", 6, 2) leads to "xxxxxxxxxst"
    */
  def overlay(newString: Expression, starting: Expression, length: Expression): T =
    call(OVERLAY, toExpr, newString, starting, length)

  /**
    * Returns a string with all substrings that match the regular expression consecutively
    * being replaced.
    */
  def regexpReplace(regex: Expression, replacement: Expression): T =
    call(REGEXP_REPLACE, toExpr, regex, replacement)

  /**
    * Returns a string extracted with a specified regular expression and a regex match group
    * index.
    */
  def regexpExtract(regex: Expression, extractIndex: Expression): T =
    call(REGEXP_EXTRACT, toExpr, regex, extractIndex)

  /**
    * Returns a string extracted with a specified regular expression.
    */
  def regexpExtract(regex: Expression): T =
    call(REGEXP_EXTRACT, toExpr, regex)

  /**
    * Returns the base string decoded with base64.
    */
  def fromBase64(): T = call(FROM_BASE64, toExpr)

  /**
    * Returns the base64-encoded result of the input string.
    */
  def toBase64(): T = call(TO_BASE64, toExpr)

  /**
    * Returns a string that removes the left whitespaces from the given string.
    */
  def ltrim(): T = call(LTRIM, toExpr)

  /**
    * Returns a string that removes the right whitespaces from the given string.
    */
  def rtrim(): T = call(RTRIM, toExpr)

  /**
    * Returns a string that repeats the base string n times.
    */
  def repeat(n: Expression): T = call(REPEAT, toExpr, n)

  // Temporal operations

  /**
    * Parses a date string in the form "yyyy-MM-dd" to a SQL Date.
    */
  def toDate: T = call(CAST, toExpr, typeLiteral(SqlTimeTypeInfo.DATE))

  /**
    * Parses a time string in the form "HH:mm:ss" to a SQL Time.
    */
  def toTime: T = call(CAST, toExpr, typeLiteral(SqlTimeTypeInfo.TIME))

  /**
    * Parses a timestamp string in the form "yyyy-MM-dd HH:mm:ss[.SSS]" to a SQL Timestamp.
    */
  def toTimestamp: T =
    call(CAST, toExpr, typeLiteral(SqlTimeTypeInfo.TIMESTAMP))

  /**
    * Extracts parts of a time point or time interval. Returns the part as a long value.
    *
    * e.g. "2006-06-05".toDate.extract(DAY) leads to 5
    */
  def extract(timeIntervalUnit: TimeIntervalUnit): T =
    call(EXTRACT, symbol(timeIntervalUnit), toExpr)

  /**
    * Rounds down a time point to the given unit.
    *
    * e.g. "12:44:31".toDate.floor(MINUTE) leads to 12:44:00
    */
  def floor(timeIntervalUnit: TimeIntervalUnit): T =
    call(FLOOR, symbol(timeIntervalUnit), toExpr)

  /**
    * Rounds up a time point to the given unit.
    *
    * e.g. "12:44:31".toDate.ceil(MINUTE) leads to 12:45:00
    */
  def ceil(timeIntervalUnit: TimeIntervalUnit): T =
    call(CEIL, symbol(timeIntervalUnit), toExpr)

  // Advanced type helper functions

  /**
    * Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by name and
    * returns it's value.
    *
    * @param name name of the field (similar to Flink's field expressions)
    * @return value of the field
    */
  def get(name: String): T = call(GET, toExpr, valueLiteral(name))

  /**
    * Accesses the field of a Flink composite type (such as Tuple, POJO, etc.) by index and
    * returns it's value.
    *
    * @param index position of the field
    * @return value of the field
    */
  def get(index: Int): T = call(GET, toExpr, valueLiteral(index))

  /**
    * Converts a Flink composite type (such as Tuple, POJO, etc.) and all of its direct subtypes
    * into a flat representation where every subtype is a separate field.
    */
  def flatten(): T = call(FLATTEN, toExpr)

  /**
    * Accesses the element of an array or map based on a key or an index (starting at 1).
    *
    * @param index key or position of the element (array index starting at 1)
    * @return value of the element
    */
  def at(index: Expression): T = call(AT, toExpr, index)

  /**
    * Returns the number of elements of an array or number of entries of a map.
    *
    * @return number of elements or entries
    */
  def cardinality(): T = call(CARDINALITY, toExpr)

  /**
    * Returns the sole element of an array with a single element. Returns null if the array is
    * empty. Throws an exception if the array has more than one element.
    *
    * @return the first and only element of an array with a single element
    */
  def element(): T = call(ARRAY_ELEMENT, toExpr)

  // Time definition

  /**
    * Declares a field as the rowtime attribute for indicating, accessing, and working in
    * Flink's event time.
    */
  def rowtime: T = call(ROWTIME, toExpr)

  /**
    * Declares a field as the proctime attribute for indicating, accessing, and working in
    * Flink's processing time.
    */
  def proctime: T = call(PROCTIME, toExpr)

  // Hash functions

  /**
    * Returns the MD5 hash of the string argument; null if string is null.
    *
    * @return string of 32 hexadecimal digits or null
    */
  def md5(): T = call(MD5, toExpr)

  /**
    * Returns the SHA-1 hash of the string argument; null if string is null.
    *
    * @return string of 40 hexadecimal digits or null
    */
  def sha1(): T = call(SHA1, toExpr)

  /**
    * Returns the SHA-224 hash of the string argument; null if string is null.
    *
    * @return string of 56 hexadecimal digits or null
    */
  def sha224(): T = call(SHA224, toExpr)

  /**
    * Returns the SHA-256 hash of the string argument; null if string is null.
    *
    * @return string of 64 hexadecimal digits or null
    */
  def sha256(): T = call(SHA256, toExpr)

  /**
    * Returns the SHA-384 hash of the string argument; null if string is null.
    *
    * @return string of 96 hexadecimal digits or null
    */
  def sha384(): T = call(SHA384, toExpr)

  /**
    * Returns the SHA-512 hash of the string argument; null if string is null.
    *
    * @return string of 128 hexadecimal digits or null
    */
  def sha512(): T = call(SHA512, toExpr)

  /**
    * Returns the hash for the given string expression using the SHA-2 family of hash
    * functions (SHA-224, SHA-256, SHA-384, or SHA-512).
    *
    * @param hashLength bit length of the result (either 224, 256, 384, or 512)
    * @return string or null if one of the arguments is null.
    */
  def sha2(hashLength: Expression): T = call(SHA2, toExpr, hashLength)

}
