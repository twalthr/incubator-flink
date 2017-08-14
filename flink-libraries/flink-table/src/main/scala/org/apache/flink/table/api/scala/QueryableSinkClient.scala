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

import java.util.concurrent.TimeUnit

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
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.typeutils.FieldTypeUtils
import org.apache.flink.types.Row

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
  * Client for connecting to a [[org.apache.flink.table.sinks.QueryableTableSink]].
  */
class QueryableSinkClient(
    jobManagerAddress: String,
    jobManagerPort: Int)
  extends org.apache.flink.table.api.QueryableSinkClient(
    jobManagerAddress,
    jobManagerPort) {

  /**
    * Creates a query to the queryable sink of the given job. The query will use the provided type
    * as the key. If your Table API/SQL query has multiple keys (that you can define using
    * tuple or row types), please use [[queryByKeys()]].
    *
    * @param jobId the job to be queried
    * @param sinkName the queryable sink to be queried
    * @param keyType the single key type (as defined in your Table API/SQL query)
    * @param valueType the result type (as passed to the queryable sink)
    * @tparam K key type
    * @tparam V value type
    * @return [[SinkQuery]] that allows to send requests to the queryable sink
    */
  def queryByKey[K, V]
      (jobId: String, sinkName: String)
      (implicit keyType: TypeInformation[K], valueType: TypeInformation[V])
    : SinkQuery[K, V] = {

    queryInternal(jobId, sinkName, keyType, valueType, unnestKey = false)
  }

  /**
    * Creates a query to the queryable sink of the given job. The query will use the provided type
    * as a set of keys. If your Table API/SQL query has multiple keys, you can define a tuple or
    * row type with all composite key types. If you only defined one key in your Table API/SQL
    * query, you can use [[queryByKey()]].
    *
    * @param jobId the job to be queried
    * @param sinkName the queryable sink to be queried
    * @param keyType the type consisting of multiple key types
    *                (as defined in your Table API/SQL query)
    * @param valueType the result type (as passed to the queryable sink)
    * @tparam K key type
    * @tparam V value type
    * @return [[SinkQuery]] that allows to send requests to the queryable sink
    */
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

    private val DEFAULT_TIMEOUT = 10000L
    private val DEFAULT_RETRIES = 5
    private val DEFAULT_TIME_BETWEEN_RETRIES = 5000L

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
      * Requests the given key for this queryable table sink. The request happens asynchronous.
      * A [[Future]] is returned which can be used for retrieving the result or an
      * exception in case of failures.
      *
      * @param key key specified in the Table API/SQL query
      * @return future of the value for the given key
      */
    def requestAsynchronous(key: K): Future[V] = {
      val (keyHash, serializedKey) = getKey(key)
      getRequestFuture(keyHash, serializedKey)
    }

    /**
      * Requests the given key for this queryable table sink. The request happens synchronous.
      * The calling thread will be blocked until the number of retries has passed. The result
      * value is returned or a [[TableException]] is thrown in case of failures.
      *
      * This method uses default values for timeout per request, number of retries, and time
      * between retries.
      *
      * @param key key specified in the Table API/SQL query
      * @return value for the given key
      */
    @throws(classOf[Exception])
    def requestSynchronous(key: K): V = {
      requestSynchronous(key, DEFAULT_TIMEOUT, DEFAULT_RETRIES, DEFAULT_TIME_BETWEEN_RETRIES)
    }

    /**
      * Requests the given key for this queryable table sink. The request happens synchronous.
      * The calling thread will be blocked until the number of retries has passed. The result
      * value is returned or a [[TableException]] is thrown in case of failures.
      *
      * This method allows to specify parameters for timeout per request, number of retries, and
      * time between retries.
      *
      * @param key key specified in the Table API/SQL query
      * @param timeout timeout per request in milliseconds
      * @param retries number of retries before an exception will be thrown
      * @param timeBetweenRetries time between retries in milliseconds
      * @return value for the given key
      */
    @throws(classOf[Exception])
    def requestSynchronous(key: K, timeout: Long, retries: Int, timeBetweenRetries: Long): V = {
      require(retries >= 0, "Number of retries must not be negative.")
      require(timeout >= 0, "Timeout must not be negative.")

      val (keyHash, serializedKey) = getKey(key)

      var result: Option[Either[V, Throwable]] = None

      var retry = 0
      while (retry < retries) {
        val future = getRequestFuture(keyHash, serializedKey)
        future.onComplete {
          case Success(r) => result = Some(Left(r))
          case Failure(e) => result = Some(Right(e))
        }
        Await.ready(future, Duration(timeout, TimeUnit.MILLISECONDS))
        result match {
          case Some(Left(r)) => return r
          case Some(Right(e)) if retry == retries - 1 =>
            // last retry throw an exception
            throw new TableException("Could not retrieve result from queryable sink.", e)
          case _ => // no result
        }
        Thread.sleep(timeBetweenRetries)
        retry += 1
      }
      throw new TableException(
        "Could not retrieve result or causing exception from queryable sink. " +
          "Maybe the timeouts are too strict.")
    }

    private def getKey(key: K): (Int, Array[Byte]) = {
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

      (key.hashCode(), serializedKey)
    }

    private def getRequestFuture(keyHash: Int, serializedKey: Array[Byte]): Future[V] = {
      val future = client.getKvState(jobID, sinkName, keyHash, serializedKey)

      future.transform(
        KvStateRequestSerializer.deserializeValue(_, valueSerializer),
        new TableException("Could not retrieve result from queryable sink.", _))
    }
  }
}
