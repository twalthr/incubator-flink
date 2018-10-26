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

import java.io.Serializable

import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.{PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.typeutils._
import org.apache.flink.table.api.{TableException, Types, ValidationException}
import org.apache.flink.table.descriptors.DescriptorProperties.normalizeTypeInfo
import org.apache.flink.util.InstantiationUtil

import _root_.scala.language.implicitConversions
import _root_.scala.util.parsing.combinator.{JavaTokenParsers, PackratParsers}

/**
  * Utilities to convert [[org.apache.flink.api.common.typeinfo.TypeInformation]] into a
  * string representation and back.
  */
object TypeStringUtils extends JavaTokenParsers with PackratParsers {
  case class Keyword(key: String)

  // convert the keyword into an case insensitive Parser
  implicit def keyword2Parser(kw: Keyword): Parser[String] = {
    ("""(?i)\Q""" + kw.key + """\E""").r
  }

  lazy val VARCHAR: Keyword = Keyword("VARCHAR")
  lazy val STRING: Keyword = Keyword("STRING")
  lazy val BOOLEAN: Keyword = Keyword("BOOLEAN")
  lazy val BYTE: Keyword = Keyword("BYTE")
  lazy val TINYINT: Keyword = Keyword("TINYINT")
  lazy val SHORT: Keyword = Keyword("SHORT")
  lazy val SMALLINT: Keyword = Keyword("SMALLINT")
  lazy val INT: Keyword = Keyword("INT")
  lazy val LONG: Keyword = Keyword("LONG")
  lazy val BIGINT: Keyword = Keyword("BIGINT")
  lazy val FLOAT: Keyword = Keyword("FLOAT")
  lazy val DOUBLE: Keyword = Keyword("DOUBLE")
  lazy val DECIMAL: Keyword = Keyword("DECIMAL")
  lazy val SQL_DATE: Keyword = Keyword("SQL_DATE")
  lazy val DATE: Keyword = Keyword("DATE")
  lazy val SQL_TIME: Keyword = Keyword("SQL_TIME")
  lazy val TIME: Keyword = Keyword("TIME")
  lazy val SQL_TIMESTAMP: Keyword = Keyword("SQL_TIMESTAMP")
  lazy val TIMESTAMP: Keyword = Keyword("TIMESTAMP")
  lazy val ROW: Keyword = Keyword("ROW")
  lazy val ANY: Keyword = Keyword("ANY")
  lazy val POJO: Keyword = Keyword("POJO")
  lazy val MAP: Keyword = Keyword("MAP")
  lazy val MULTISET: Keyword = Keyword("MULTISET")
  lazy val PRIMITIVE_ARRAY: Keyword = Keyword("PRIMITIVE_ARRAY")
  lazy val OBJECT_ARRAY: Keyword = Keyword("OBJECT_ARRAY")

  lazy val qualifiedName: Parser[String] =
    """\p{javaJavaIdentifierStart}[\p{javaJavaIdentifierPart}.]*""".r

  lazy val base64Url: Parser[String] =
    """[A-Za-z0-9_-]*""".r

  // keep parenthesis to ensure backward compatibility (this can be dropped after Flink 1.7)
  lazy val leftBracket: PackratParser[(String)] =
    "(" | "<"

  lazy val rightBracket: PackratParser[(String)] =
    ")" | ">"

  lazy val atomic: PackratParser[TypeInformation[_]] =
    (VARCHAR | STRING) ^^ { e => Types.STRING } |
    BOOLEAN ^^ { e => Types.BOOLEAN } |
    (TINYINT | BYTE) ^^ { e => Types.BYTE } |
    (SMALLINT | SHORT) ^^ { e => Types.SHORT } |
    INT ^^ { e => Types.INT } |
    (BIGINT | LONG) ^^ { e => Types.LONG } |
    FLOAT ^^ { e => Types.FLOAT } |
    DOUBLE ^^ { e => Types.DOUBLE } |
    DECIMAL ^^ { e => Types.DECIMAL } |
    (DATE | SQL_DATE) ^^ { e => Types.SQL_DATE.asInstanceOf[TypeInformation[_]] } |
    (TIMESTAMP | SQL_TIMESTAMP) ^^ { e => Types.SQL_TIMESTAMP } |
    (TIME | SQL_TIME) ^^ { e => Types.SQL_TIME }

  lazy val escapedFieldName: PackratParser[String] = stringLiteral ^^ { s =>
    val unquoted = s.substring(1, s.length - 1)
    StringEscapeUtils.unescapeJava(unquoted)
  }

  lazy val fieldName: PackratParser[String] = escapedFieldName | ident

  lazy val field: PackratParser[(String, TypeInformation[_])] =
    fieldName ~ typeInfo ^^ {
      case name ~ info => (name, info)
    }



  lazy val typeInfo: PackratParser[TypeInformation[_]] =
    namedRow | unnamedRow | any | generic | pojo | atomic | map | array | failure("Invalid type.")

  def readTypeInfo(typeString: String): TypeInformation[_] = {
    parseAll(typeInfo, typeString) match {
      case Success(lst, _) => lst

      case NoSuccess(msg, next) =>
        throwError(msg, next)
    }
  }
}
