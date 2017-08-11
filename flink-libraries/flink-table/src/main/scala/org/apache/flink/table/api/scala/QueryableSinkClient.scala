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

import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.operators.Keys
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer
import org.apache.flink.runtime.state.{VoidNamespace, VoidNamespaceSerializer}
import org.apache.flink.streaming.util.keys.KeySelectorUtil
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.typeutils.FieldTypeUtils
import org.apache.flink.types.Row

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

/**
  * Client for connecting to a [[org.apache.flink.table.sinks.QueryableTableSink]].
  */
class QueryableSinkClient(
    jobManagerAddress: String,
    jobManagerPort: Int)
  extends org.apache.flink.table.api.QueryableSinkClient(
    jobManagerAddress,
    jobManagerPort) {

  def queryByKey[K, V]
      (jobId: String, sinkName: String)
      (implicit keyType: TypeInformation[K], valueType: TypeInformation[V])
    : SinkQuery[K, V] = {

    queryInternal(jobId, sinkName, keyType, valueType, unnestKey = false)
  }

  def queryByKeys[K, V]
      (jobId: String, sinkName: String)
      (implicit keyType: TypeInformation[K], valueType: TypeInformation[V])
    : SinkQuery[K, V] = {

    queryInternal(jobId, sinkName, keyType, valueType, unnestKey = true)
  }

  private def queryInternal[K, V](
      jobId: String,
      sinkName: String,
      keyType: TypeInformation[K],
      valueType: TypeInformation[V],
      unnestKey: Boolean)
    : SinkQuery[K, V] = {

    FieldTypeUtils.validateType(keyType)
    FieldTypeUtils.validateType(valueType)

    new SinkQuery(
      JobID.fromHexString(jobId),
      sinkName,
      keyType,
      valueType,
      unnestKey)
  }

  class SinkQuery[K, V](
    private val jobID: JobID,
    private val sinkName: String,
    private val keyType: TypeInformation[K],
    private val valueType: TypeInformation[V],
    private val unnestKey: Boolean) {

    private val keyFieldTypes: Seq[TypeInformation[_]] = if (unnestKey) {
      FieldTypeUtils.getFieldTypes(keyType)
    } else {
      Seq(keyType)
    }

    private val keySelector: Option[KeySelector[K, Tuple]] = if (unnestKey) {

        // determine key selector to unnest key
        keyType match {

          // no unnesting necessary
          case _: RowTypeInfo =>
            None

          // arrays
          case _: BasicArrayTypeInfo[_, _] | _: PrimitiveArrayTypeInfo[_] =>
            val allFields = (0 until keyType.getArity).toArray
            Some(KeySelectorUtil.getSelectorForArray(allFields, keyType))

          // composite types
          case _: CompositeType[_] =>
            val exprKeys = new Keys.ExpressionKeys[K](keyType)
            Some(KeySelectorUtil.getSelectorForKeys(exprKeys, keyType, execConfig))

          case _ =>
            throw new ValidationException("Composite key such as ROW(STRING, STRING) expected.")
        }
      } else {
      None
    }

    private val keyRowType = new RowTypeInfo(keyFieldTypes: _*)

    private val keyRowSerializer = keyRowType.createSerializer(execConfig)

    private val valueSerializer = valueType.createSerializer(execConfig)

    /**
      * Requests the given key for this queryable table sink.
      *
      * @param key key specified in the Table API/SQL query
      * @return future of the value for the given key
      */
    def request(key: K): Future[V] = {

      val keyRow = if (unnestKey) {
        keySelector match {
          // with key selector
          case Some(ks) =>
            // convert Tuple to Row
            val keyTuple = ks.getKey(key)
            val len = keyTuple.getArity
            val row = new Row(len)
            var i = 0
            while (i < len) {
              row.setField(i, keyTuple.getField(i))
              i += 1
            }
            row

          // without key selector
          case None =>
            Row.of(key.asInstanceOf[AnyRef])
        }
      } else {
        Row.of(key.asInstanceOf[AnyRef])
      }

      val serializedKey = KvStateRequestSerializer.serializeKeyAndNamespace(
        keyRow,
        keyRowSerializer,
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE)

      val future = client.getKvState(jobID, sinkName, key.hashCode(), serializedKey)

      future.map { k =>
        val rel = k
        KvStateRequestSerializer.deserializeValue(rel, valueSerializer)
      }
    }
  }
}
