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

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.deployment.StandaloneClusterDescriptor;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.BatchQueryConfig;
import org.apache.flink.table.api.QueryConfig;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.config.Deployment;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.Execution;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceFactoryService;
import org.apache.flink.types.Row;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Executor that performs the Flink communication locally. The calls are blocking depending on the
 * response time to the Flink cluster. Flink jobs are not blocking.
 */
public class LocalExecutor implements Executor {

	private final Environment environment;
	private final List<URL> dependencies;
	private final Configuration flinkConfig;
	private final ResultStore resultStore;

	public LocalExecutor(Environment environment, List<URL> jars, List<URL> libraries) {
		this.environment = environment;

		try {
			// find the configuration directory
			final String flinkConfigDir = CliFrontend.getConfigurationDirectoryFromEnv();

			// load the global configuration
			flinkConfig = GlobalConfiguration.loadConfiguration(flinkConfigDir);
		} catch (Exception e) {
			throw new SqlClientException("Could not load Flink configuration.", e);
		}

		dependencies = new ArrayList<>();
		try {
			// find jar files
			for (URL url : jars) {
				JobWithJars.checkJarFile(url);
				dependencies.add(url);
			}

			// find jar files in library directories
			for (URL libUrl : libraries) {
				final File dir = new File(libUrl.toURI());
				if (!dir.isDirectory() || !dir.canRead()) {
					throw new SqlClientException("Directory cannot be read: " + dir);
				}
				final File[] files = dir.listFiles();
				if (files == null) {
					throw new SqlClientException("Directory cannot be read: " + dir);
				}
				for (File f : files) {
					// only consider jars
					if (f.isFile() && f.getAbsolutePath().toLowerCase().endsWith(".jar")) {
						final URL url = f.toURI().toURL();
						JobWithJars.checkJarFile(url);
						dependencies.add(url);
					}
				}
			}
		} catch (Exception e) {
			throw new SqlClientException("Could not load all required JAR files.", e);
		}

		resultStore = new ResultStore(flinkConfig);
	}

	@Override
	public void start() {
		// nothing to do yet
	}

	@Override
	public List<String> listTables(SessionContext context) throws SqlExecutionException {
		final Environment env = getEnvironment(context);
		final TableEnvironment tableEnv = getTableEnvironment(env);
		return Arrays.asList(tableEnv.listTables());
	}

	@Override
	public TableSchema getTableSchema(SessionContext context, String name) throws SqlExecutionException {
		final Environment env = getEnvironment(context);
		final TableEnvironment tableEnv = getTableEnvironment(env);
		try {
			return tableEnv.scan(name).getSchema();
		} catch (TableException e) {
			return null; // no table with this name found
		}
	}

	@Override
	public String explainStatement(SessionContext context, String statement) throws SqlExecutionException {
		final Environment env = getEnvironment(context);
		final TableEnvironment tableEnv = getTableEnvironment(env);
		try {
			final Table table = tableEnv.sqlQuery(statement);
			return tableEnv.explain(table);
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}
	}

	@Override
	public ResultDescriptor executeQuery(SessionContext context, String query) throws SqlExecutionException {
		final Environment env = getEnvironment(context);

		// deployment
		final ClusterClient<?> clusterClient = prepareDeployment(env.getDeployment());
		clusterClient.setDetached(true);

		// initialize result
		final DynamicResult result = resultStore.createResult(env);

		// create plan with jars
		final Tuple2<FlinkPlan, TableSchema> plan = createPlan(env, query, result.getTableSink(), clusterClient);

		final ClassLoader classLoader = JobWithJars.buildUserCodeClassLoader(
			dependencies,
			Collections.emptyList(),
			this.getClass().getClassLoader());

		// run
		final JobSubmissionResult jobResult;
		try {
			jobResult = clusterClient.run(plan.f0, dependencies, Collections.emptyList(), classLoader);
		} catch (ProgramInvocationException e) {
			throw new SqlExecutionException("Could not execute table program.", e);
		}

		final String jobId = jobResult.getJobID().toString();

		// store the result
		resultStore.storeResult(jobId, result);

		return new ResultDescriptor(jobId, plan.f1);
	}

	@Override
	public int snapshotResult(String resultId, int pageSize) throws SqlExecutionException {
		final DynamicResult result = resultStore.getResult(resultId);
		if (result == null) {
			throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
		}
		return result.snapshot(pageSize);
	}

	@Override
	public List<Row> retrieveResult(String resultId, int page) throws SqlExecutionException {
		final DynamicResult result = resultStore.getResult(resultId);
		if (result == null) {
			throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
		}
		return result.retrieve(page);
	}

	// --------------------------------------------------------------------------------------------

	private List<URL> collectJars() {
		return Collections.emptyList();
	}

	private Tuple2<FlinkPlan, TableSchema> createPlan(Environment env, String query, TableSink<?> sink, ClusterClient<?> clusterClient) {
		final TableEnvironment tableEnv = getTableEnvironment(env);

		// parse and validate query
		final Table table;
		try {
			table = tableEnv.sqlQuery(query);
		} catch (Throwable t) {
			// catch everything such that the query does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}

		// specify sink
		final QueryConfig queryConfig = getQueryConfig(env.getExecution());
		table.writeToSink(sink, queryConfig);

		// extract job graph
		if (env.getExecution().isStreamingExecution()) {
			final FlinkPlan plan = ((StreamTableEnvironment) tableEnv).execEnv().getStreamGraph();
			return Tuple2.of(plan, table.getSchema());
		} else {
			final int parallelism = env.getExecution().getParallelism();

			// create plan
			final Plan plan = ((BatchTableEnvironment) tableEnv).execEnv().createProgramPlan();
			final Optimizer compiler = new Optimizer(new DataStatistics(), new DefaultCostEstimator(),
				clusterClient.getFlinkConfiguration());
			final FlinkPlan optimizedPlan = ClusterClient.getOptimizedPlan(compiler, plan, parallelism);
			return Tuple2.of(optimizedPlan, table.getSchema());
		}
	}

	private ClusterClient<?> prepareDeployment(Deployment deploy) {

		// change some configuration options for being more responsive
		flinkConfig.setString(AkkaOptions.LOOKUP_TIMEOUT, deploy.getResponseTimeout() + " ms");
		flinkConfig.setString(AkkaOptions.CLIENT_TIMEOUT, deploy.getResponseTimeout() + " ms");

		// get cluster client
		final ClusterClient<?> clusterClient;
		if (deploy.isStandaloneDeployment()) {
			clusterClient = getStandaloneClusterClient(flinkConfig);
			clusterClient.setPrintStatusDuringExecution(false);
		} else {
			throw new SqlExecutionException("Unsupported deployment.");
		}

		return clusterClient;
	}

	private ClusterClient<?> getStandaloneClusterClient(Configuration configuration) {
		final ClusterDescriptor<StandaloneClusterId> descriptor = new StandaloneClusterDescriptor(configuration);
		try {
			return descriptor.retrieve(StandaloneClusterId.getInstance());
		} catch (ClusterRetrieveException e) {
			throw new SqlExecutionException("Could not retrieve standalone cluster.", e);
		}
	}

	private Environment getEnvironment(SessionContext context) {
		return Environment.merge(environment, context.getEnvironment());
	}

	private TableEnvironment getTableEnvironment(Environment env) {
		try {
			final TableEnvironment tableEnv;
			if (env.getExecution().isStreamingExecution()) {
				final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
				execEnv.setParallelism(env.getExecution().getParallelism());
				execEnv.setMaxParallelism(env.getExecution().getMaxParallelism());
				tableEnv = StreamTableEnvironment.getTableEnvironment(execEnv);
			} else {
				final ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
				execEnv.setParallelism(env.getExecution().getParallelism());
				tableEnv = BatchTableEnvironment.getTableEnvironment(execEnv);
			}

			env.getSources().forEach((name, source) -> {
				TableSource<?> tableSource = TableSourceFactoryService.findTableSourceFactory(source);
				tableEnv.registerTableSource(name, tableSource);
			});

			return tableEnv;
		} catch (Exception e) {
			throw new SqlExecutionException("Could not create table environment.", e);
		}
	}

	private QueryConfig getQueryConfig(Execution exec) {
		if (exec.isStreamingExecution()) {
			final StreamQueryConfig config = new StreamQueryConfig();
			final long minRetention = exec.getMinStateRetention();
			final long maxRetention = exec.getMaxStateRetention();
			config.withIdleStateRetentionTime(Time.milliseconds(minRetention), Time.milliseconds(maxRetention));
			return config;
		} else {
			return new BatchQueryConfig();
		}
	}
}
