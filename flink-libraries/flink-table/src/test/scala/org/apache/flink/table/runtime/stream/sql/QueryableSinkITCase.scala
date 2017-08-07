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
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration._
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.testingUtils.TestingCluster
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.runtime.utils.StreamITCase
import org.junit.Assert.fail
import org.apache.flink.api.scala._
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.api.scala._
import org.junit.{AfterClass, BeforeClass, Test}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.{Deadline, FiniteDuration}


class QueryableSinkITCase {

  @Test
  def testSingleKeyPojo(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(1)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 1000))

    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Hi1"))
    data.+=((1, 2L, "Hi2"))
    data.+=((1, 5L, "Hi3"))
    data.+=((2, 7L, "Hi5"))
    data.+=((1, 9L, "Hi6"))
    data.+=((1, 8L, "Hi8"))

    val table = env.fromCollection(data).toTable(tEnv, 'amount, 'key, 'value)
    tEnv.registerTable("MyTable", table)

    tEnv.sql("SELECT key, SUM(amount) FROM MyTable GROUP BY key").toQueryableSink[(Long, Int)]("TestSink")

    // submit the job graph
    val jobGraph = env.getStreamGraph.getJobGraph
    QueryableSinkITCase.cluster.submitJobDetached(jobGraph)

    // start querying
    val deadline = QueryableSinkITCase.TEST_TIMEOUT.fromNow

    val jobId = jobGraph.getJobID

    val config = QueryableSinkITCase.cluster.configuration

    val client = new QueryableSinkClient(
      config.getString(JobManagerOptions.ADDRESS, "localhost"),
      config.getInteger(JobManagerOptions.PORT, 0))

    Thread.sleep(10000)

    val query = client.query[Long, (Long, Int)](jobId.toString, "TestSink")



    val future = query.request(9)

    val value = Await.result(future, deadline.timeLeft)

    ???
  }

}

object QueryableSinkITCase {

  private val TEST_TIMEOUT = new FiniteDuration(100, TimeUnit.SECONDS)
  private val QUERY_RETRY_DELAY = new FiniteDuration(100, TimeUnit.MILLISECONDS)

  private var testActorSystem: ActorSystem = _

  private val NUM_TMS = 2
  private val NUM_SLOTS_PER_TM = 4
  private val NUM_SLOTS = NUM_TMS * NUM_SLOTS_PER_TM

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
}
