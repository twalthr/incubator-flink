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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer
import org.apache.flink.runtime.state.{VoidNamespace, VoidNamespaceSerializer}
import org.apache.flink.table.api.TableEnvironment

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

  def query[K, V](jobId: String, sinkName: String)
      (implicit keyType: TypeInformation[K], valueType: TypeInformation[V])
    : SinkQuery[K, V] = {

    queryInternal(jobId, sinkName, keyType, valueType)
  }

  private def queryInternal[K, V](
      jobId: String,
      sinkName: String,
      keyType: TypeInformation[K],
      valueType: TypeInformation[V])
    : SinkQuery[K, V] = {

    TableEnvironment.validateType(keyType)
    TableEnvironment.validateType(valueType)

    new SinkQuery(
      JobID.fromHexString(jobId),
      sinkName,
      keyType.createSerializer(execConfig),
      valueType.createSerializer(execConfig))
  }

  class SinkQuery[K, V](
    private val jobID: JobID,
    private val sinkName: String,
    private val keySerializer: TypeSerializer[K],
    private val valueSerializer: TypeSerializer[V]) {

    /**
      * Requests the given key for this queryable table sink.
      *
      * @param key key specified in the Table API/SQL query
      * @return future of the value for the given key
      */
    def request(key: K): Future[V] = {
      val serializedKey = KvStateRequestSerializer.serializeKeyAndNamespace(
        key,
        keySerializer,
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE)

      val future = client.getKvState(jobID, sinkName, key.hashCode(), serializedKey)

      future.map(KvStateRequestSerializer.deserializeValue(_, valueSerializer))
    }
  }
}
