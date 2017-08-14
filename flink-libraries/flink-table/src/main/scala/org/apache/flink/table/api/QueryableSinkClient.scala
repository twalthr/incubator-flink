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

package org.apache.flink.table.api

import _root_.java.util.concurrent.Executors

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.configuration.{Configuration, JobManagerOptions}
import org.apache.flink.runtime.highavailability.{HighAvailabilityServices, HighAvailabilityServicesUtils}
import org.apache.flink.runtime.query.QueryableStateClient
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
  * Client for connecting to a [[org.apache.flink.table.sinks.QueryableTableSink]].
  */
abstract class QueryableSinkClient(
    private val jobManagerAddress: String,
    private val jobManagerPort: Int) {

  protected val config: Configuration = new Configuration()
  config.setString(JobManagerOptions.ADDRESS, jobManagerAddress)
  config.setInteger(JobManagerOptions.PORT, jobManagerPort)

  protected val haServices: HighAvailabilityServices =
    HighAvailabilityServicesUtils.createHighAvailabilityServices(
      config,
      Executors.newSingleThreadScheduledExecutor(),
      HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION)

  protected val client = new QueryableStateClient(config, haServices)

  protected val execConfig: ExecutionConfig =
    StreamExecutionEnvironment.getExecutionEnvironment.getConfig

  /**
    * Shuts down the client.
    */
  def shutdown(): Unit = {
    client.shutDown()
  }
}
