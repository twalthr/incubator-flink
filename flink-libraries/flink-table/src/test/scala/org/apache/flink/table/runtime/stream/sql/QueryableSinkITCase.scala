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

package org.apache.flink.table.runtime.stream.sql


import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala._
import org.apache.flink.configuration._
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.messages.JobManagerMessages
import org.apache.flink.runtime.testingUtils.TestingCluster
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.stream.sql.QueryableSinkITCase.TestAscendingValueSource
import org.junit.Assert.{assertEquals, fail}
import org.junit.{AfterClass, BeforeClass, Test}

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

class QueryableSinkITCase {

  @Test
  def testSingleKeyPojo(): Unit = {

    val config = QueryableSinkITCase.cluster.configuration

    // start client
    val client = new QueryableSinkClient(
      config.getString(JobManagerOptions.ADDRESS, "localhost"),
      config.getInteger(JobManagerOptions.PORT, 0))

    var jobId: JobID = null
    try {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(2)
      env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000))

      val tEnv = TableEnvironment.getTableEnvironment(env)

      val table = env.addSource(new TestAscendingValueSource(10)).toTable(tEnv, 'subtask, 'value)
      tEnv.registerTable("MyTable", table)

      tEnv
        .sql("SELECT subtask, MIN(`value`) FROM MyTable GROUP BY subtask")
        .toQueryableSink[(Int, Long)]("TestSink")

      // submit the job graph
      val jobGraph = env.getStreamGraph.getJobGraph
      QueryableSinkITCase.cluster.submitJobDetached(jobGraph)

      // start querying

      jobId = jobGraph.getJobID

      val query = client.queryByKey[Int, (Int, Long)](jobId.toString, "TestSink")

      val result = query.requestSynchronous(
        key = 0,
        timeout = QueryableSinkITCase.TEST_TIMEOUT.toMillis,
        retries = 10,
        timeBetweenRetries = QueryableSinkITCase.TEST_TIMEOUT.toMillis)

      assertEquals(result, (0, 0))
    } finally {
      // free cluster resources
      if (jobId != null) {
        val cancellation = QueryableSinkITCase.cluster
          .getLeaderGateway(QueryableSinkITCase.TEST_TIMEOUT)
          .ask(JobManagerMessages.CancelJob(jobId), QueryableSinkITCase.TEST_TIMEOUT)
        Await.ready(cancellation, QueryableSinkITCase.TEST_TIMEOUT)
      }
      client.shutdown()
    }
  }
}

object QueryableSinkITCase {

  val TEST_TIMEOUT = new FiniteDuration(5, TimeUnit.SECONDS)

  private var testActorSystem: ActorSystem = _

  private val NUM_TMS = 2
  private val NUM_SLOTS_PER_TM = 2

  /**
    * Shared between all the test. Make sure to have at least NUM_SLOTS
    * available after your test finishes, e.g. cancel the job you submitted.
    */
  private var cluster: org.apache.flink.runtime.testingUtils.TestingCluster = _

  @BeforeClass
  def setup(): Unit = {
    try {
      val config = new Configuration
      config.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 4L)
      config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS)
      config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_SLOTS_PER_TM)
      config.setInteger(QueryableStateOptions.CLIENT_NETWORK_THREADS, 1)
      config.setBoolean(QueryableStateOptions.SERVER_ENABLE, true)
      config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 1)
      cluster = new TestingCluster(config, false)
      cluster.start(true)
      testActorSystem = AkkaUtils.createDefaultActorSystem()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        fail(e.getMessage)
    }
  }

  @AfterClass
  def tearDown(): Unit = {
    try {
      cluster.stop()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        fail(e.getMessage)
    }
    if (testActorSystem != null) {
      testActorSystem.shutdown()
    }
  }

  private class TestAscendingValueSource(val maxValue: Long)
      extends RichParallelSourceFunction[Tuple2[Integer, Long]] {

    @volatile
    private var isRunning = true

    @throws[Exception]
    override def run(ctx: SourceFunction.SourceContext[Tuple2[Integer, Long]]): Unit = {
      val key = getRuntimeContext.getIndexOfThisSubtask
      val record = new Tuple2[Integer, Long](key, 0L)
      var currentValue = 0
      while (isRunning && currentValue <= maxValue) {
        ctx.getCheckpointLock.synchronized {
          record.f1 = currentValue
          ctx.collect(record)
        }
        currentValue += 1
      }
      while (isRunning) {
        this.synchronized {
          this.wait()
        }
      }
    }

    override def cancel(): Unit = {
      isRunning = false
      this.synchronized {
        this.notifyAll()
      }
    }
  }
}
