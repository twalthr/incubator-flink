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

import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.fun.{SqlMinMaxAggFunction, SqlCountAggFunction}
import org.apache.flink.api.table.expressions._

object AggCallToExpr {

  def translate(aggCall: AggregateCall, aggFields: Seq[Expression]): Expression = {
    val function = translateToAggExpr(aggCall, aggFields)
    Naming(function, aggCall.getName)
  }

  private def translateToAggExpr(aggCall: AggregateCall, aggFields: Seq[Expression])
      : Expression = aggCall.getAggregation match {
    case count: SqlCountAggFunction => Count(aggFields(0))
    case min: SqlMinMaxAggFunction if min.isMin => Min(aggFields(aggCall.getArgList.get(0)))
    case max: SqlMinMaxAggFunction if !max.isMin => Max(aggFields(aggCall.getArgList.get(0)))
    case _ => ???
  }

}
