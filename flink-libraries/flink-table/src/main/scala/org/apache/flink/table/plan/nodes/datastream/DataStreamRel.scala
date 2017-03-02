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

package org.apache.flink.table.plan.nodes.datastream


import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.{RelDataType, RelRecordType}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.StreamTableEnvironment
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.FlinkRelNode
import org.apache.flink.types.Row

import scala.collection.JavaConversions._

trait DataStreamRel extends RelNode with FlinkRelNode {

  /**
    * Translates the FlinkRelNode into a Flink operator.
    *
    * @param tableEnv The [[StreamTableEnvironment]] of the translated Table.
    * @return DataStream of type [[Row]]
    */
  private[flink] def translateToPlan(tableEnv: StreamTableEnvironment) : DataStream[Row]

  def getPhysicalFieldNames: Seq[String] = {
    val fields = getRowType.getFieldList filter { field =>
      !FlinkTypeFactory.isTimeIndicatorType(field.getType)
    }
    fields.map(_.getName)
  }

  def getPhysicalFieldTypes: Seq[TypeInformation[_]] = {
    val fields = getRowType.getFieldList filter { field =>
      !FlinkTypeFactory.isTimeIndicatorType(field.getType)
    }
    fields.map(f => FlinkTypeFactory.toTypeInfo(f.getType))
  }

  def getPhysicalRowTypeInfo: TypeInformation[Row] = {
    val fields = getRowType.getFieldList filter { field =>
      !FlinkTypeFactory.isTimeIndicatorType(field.getType)
    }
    val types = fields.map(f => FlinkTypeFactory.toTypeInfo(f.getType))
    val names = fields.map(f => f.getName)
    new RowTypeInfo(types.toArray, names.toArray)
  }

  def getPhysicalRowType: RelDataType = {
    val fields = getRowType.getFieldList filter { field =>
      !FlinkTypeFactory.isTimeIndicatorType(field.getType)
    }
    new RelRecordType(fields)
  }
}

