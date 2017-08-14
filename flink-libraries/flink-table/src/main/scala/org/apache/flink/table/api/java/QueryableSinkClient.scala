package org.apache.flink.table.api.java

import java.util.concurrent.{Future, TimeUnit}

import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer
import org.apache.flink.runtime.state.{VoidNamespace, VoidNamespaceSerializer}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.typeutils.FieldTypeUtils
import org.apache.flink.types.Row

import scala.concurrent.Await
import scala.concurrent.duration.Duration

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
    * Queries the given [[org.apache.flink.table.sinks.QueryableTableSink]].
    *
    * @param jobId Job id of the job the queryable table sink belongs to
    * @param sinkName name of the queryable table sink
    * @param keyType type information for the key type specified in the Table API/SQL query
    * @param valueType type information for the value specified for the queryable table sink
    * @tparam V value type
    * @return sink query targeted to the specified queryable table sink
    */
  def queryByKeys[V](
      jobId: String,
      sinkName: String,
      keyType: TypeInformation[Row],
      valueType: TypeInformation[V])
    : SinkQuery[V] = {

    FieldTypeUtils.validateType(keyType)
    FieldTypeUtils.validateType(valueType)

    val keyTypes = FieldTypeUtils.getFieldTypes(keyType)

    val keyRowType = new RowTypeInfo(keyTypes: _*)

    new SinkQuery(
      JobID.fromHexString(jobId),
      sinkName,
      keyRowType.createSerializer(execConfig),
      valueType.createSerializer(execConfig))
  }

  class SinkQuery[V](
    private val jobID: JobID,
    private val sinkName: String,
    private val keySerializer: TypeSerializer[Row],
    private val valueSerializer: TypeSerializer[V]) {

    def request(keys: AnyRef*): Future[V] = {
      request(Row.of(keys: _*))
    }

    def request(key: Row): Future[V] = {
      val serializedKey = KvStateRequestSerializer.serializeKeyAndNamespace(
        key,
        keySerializer,
        VoidNamespace.INSTANCE,
        VoidNamespaceSerializer.INSTANCE)

      val future = client.getKvState(jobID, sinkName, key.hashCode(), serializedKey)

      new Future[V] {

        override def isCancelled: Boolean =
          throw new UnsupportedOperationException("Request cancellation is not supported.")

        override def get(): V = {
          val serializedValue = Await.result(future, Duration.Inf)
          KvStateRequestSerializer.deserializeValue(serializedValue, valueSerializer)
        }

        override def get(timeout: Long, unit: TimeUnit): V = {
          val serializedValue = Await.result(future, Duration.create(timeout, unit))
          KvStateRequestSerializer.deserializeValue(serializedValue, valueSerializer)
        }

        override def cancel(mayInterruptIfRunning: Boolean): Boolean =
          throw new UnsupportedOperationException("Request cancellation is not supported.")

        override def isDone: Boolean = future.isCompleted
      }
    }
  }
}
