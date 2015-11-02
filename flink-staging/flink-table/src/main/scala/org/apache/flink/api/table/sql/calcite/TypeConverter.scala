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

import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}

object TypeConverter {

  def typeInfoToSqlType(typeInfo: TypeInformation[_]): SqlTypeName = typeInfo match {
      case STRING_TYPE_INFO => VARCHAR
      case INT_TYPE_INFO => INTEGER
      case LONG_TYPE_INFO => BIGINT
      case DOUBLE_TYPE_INFO => DECIMAL
      case DATE_TYPE_INFO => DATE
      case _ => ??? // TODO more types
    }

  def sqlTypeToTypeInfo(sqlType: SqlTypeName): TypeInformation[_] = sqlType match {
      case VARCHAR | CHAR => STRING_TYPE_INFO
      case INTEGER => INT_TYPE_INFO
      case BIGINT => LONG_TYPE_INFO
      case DECIMAL => DOUBLE_TYPE_INFO
      case DATE => DATE_TYPE_INFO
      case _ => ??? // TODO more types
    }

}
