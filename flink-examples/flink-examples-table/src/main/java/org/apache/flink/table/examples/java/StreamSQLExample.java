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

package org.apache.flink.table.examples.java;

import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Simple example for demonstrating the use of SQL on a Stream Table in Java.
 *
 * <p>Usage: <code>StreamSQLExample --planner &lt;blink|flink&gt;</code><br>
 *
 * <p>This example shows how to:
 *  - Convert DataStreams to Tables
 *  - Register a Table under a name
 *  - Run a StreamSQL query on the registered Table
 *
 */
public class StreamSQLExample {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
		tEnv.getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, 1);

		tEnv.executeSql(
			"CREATE TABLE UserScores (name STRING, score INT)\n" +
			"WITH (\n" +
			"  'connector' = 'socket',\n" +
			"  'hostname' = 'localhost',\n" +
			"  'port' = '9999',\n" +
			"  'byte-delimiter' = '10',\n" +
			"  'format' = 'changelog-csv',\n" +
			"  'changelog-csv.column-delimiter' = '|'\n" +
			")");

		tEnv.sqlQuery("SELECT name, SUM(score) FROM UserScores GROUP BY name").execute().print();
	}
}
