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
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceFactoryService;

import java.util.Arrays;
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
	public List<String> listTables(SessionContext context) throws SqlExecutionException {
		final TableEnvironment tableEnv = getTableEnvironment(context);
		return Arrays.asList(tableEnv.listTables());
	}

	@Override
	public TableSchema getTableSchema(SessionContext context, String name) throws SqlExecutionException {
		final TableEnvironment tableEnv = getTableEnvironment(context);

		try {
			return tableEnv.scan(name).getSchema();
		} catch (TableException e) {
			return null; // no table with this name found
		}
	}

	@Override
	public String explainStatement(SessionContext context, String statement) throws SqlExecutionException {
		final TableEnvironment tableEnv = getTableEnvironment(context);
		try {
			final Table table = tableEnv.sqlQuery(statement);
			return tableEnv.explain(table);
		} catch (TableException | SqlParserException | ValidationException e) {
			throw new SqlExecutionException("Invalid SQL statement.", e);
		}
	}

	@Override
	public void executeQuery(SessionContext context, String query) throws SqlExecutionException {
		final TableEnvironment tableEnv = getTableEnvironment(context);
		final Table table;
		try {
			table = tableEnv.sqlQuery(query);
		} catch (TableException | SqlParserException | ValidationException e) {
			throw new SqlExecutionException("Invalid SQL statement.", e);
		}
	}

	// --------------------------------------------------------------------------------------------

	private TableEnvironment getTableEnvironment(SessionContext context) throws SqlExecutionException {
		try {
			final Environment env = Environment.merge(environment, context.getEnvironment());

			final TableEnvironment tableEnv;
			if (env.getExecution().isStreamingExecution()) {
				final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
				tableEnv = BatchTableEnvironment.getTableEnvironment(execEnv);
			} else {
				final ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
				tableEnv = BatchTableEnvironment.getTableEnvironment(execEnv);
			}

			environment.getSources().forEach((name, source) -> {
				TableSource<?> tableSource = TableSourceFactoryService.findTableSourceFactory(source);
				tableEnv.registerTableSource(name, tableSource);
			});

			return tableEnv;
		} catch (Exception e) {
			throw new SqlExecutionException("Could not create table environment.", e);
		}
	}
}
