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

import org.apache.flink.table.api.TableSchema;

import java.util.List;

/**
 * A gateway for communicating with Flink and other external systems.
 */
public interface Executor {

	/**
	 * Starts the executor and ensures that its is ready for commands to be executed.
	 */
	void start() throws SqlExecutionException;

	/**
	 * Lists all tables known to the executor.
	 */
	List<String> listTables(SessionContext context) throws SqlExecutionException;

	/**
	 * Returns the schema of a table or null if no table with this name exists.
	 */
	TableSchema getTableSchema(SessionContext context, String name) throws SqlExecutionException;

	/**
	 * Returns a string-based explanation about AST and execution plan of the given statement.
	 */
	String explainStatement(SessionContext context, String statement) throws SqlExecutionException;

	void executeQuery(SessionContext context, String query) throws SqlExecutionException;
}
