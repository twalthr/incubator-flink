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

package org.apache.flink.table.client.gateway;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.client.config.Environment;

import java.util.List;

/**
 * Executor that performs the Flink communication locally.
 */
public class LocalExecutor implements Executor {

	private final Environment environment;
	private final Thread executorThread;

	public LocalExecutor(Environment environment) {
		this.environment = environment;

		executorThread = new Thread("Flink SQL Client Executor");
	}

	@Override
	public void start() {
		executorThread.start();
	}

	@Override
	public List<String> listTables(SessionContext context) {
		// TODO merge session context and environment
		return null;
	}

	private BatchTableEnvironment getBatchTableEnvironment(Environment environment) {
		final ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
		final BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(execEnv);
		environment.getSources().forEach((name, source) -> {
			tableEnv.registerTableSource(name, source);
		});
	}
}
