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

package org.apache.flink.table.client.cli;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.types.Either;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * Translates the messages between the executor and CLI client. The result of the translator
 * can be printed by the CLI client.
 */
public class CliTranslator {

	private final SessionContext context;
	private final Executor executor;

	public CliTranslator(SessionContext context, Executor executor) {
		this.context = context;
		this.executor = executor;
	}

	public String translateShowTables() {
		return printOrError(() -> {
			final List<String> tables = this.executor.listTables(context);
			if (tables.size() == 0) {
				return CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY);
			} else {
				return String.join("\n", tables);
			}
		});
	}

	public String translateDescribeTable(String name) {
		return printOrError(() -> {
			final TableSchema schema = this.executor.getTableSchema(context, name);
			if (schema == null) {
				return CliStrings.messageError(CliStrings.MESSAGE_UNKNOWN_TABLE);
			}
			return schema.toString();
		});
	}

	public String translateExplainTable(String statement) {
		return printOrError(() -> this.executor.explainStatement(context, statement));
	}

	/**
	 * Returns either an result identifier or an error message.
	 */
	public Either<CliResultDescriptor, String> translateSelect(String query) {
		try {
			final ResultDescriptor resultDesc = this.executor.executeQuery(context, query);
			return Either.Left(new CliResultDescriptor(
				resultDesc.getResultId(),
				resultDesc.getResultSchema().getColumnNames()));
		} catch (SqlExecutionException e) {
			return Either.Right(CliStrings.messageError(CliStrings.MESSAGE_SQL_EXECUTION_ERROR, e));
		}
	}

	public List<String[]> translateResultRetrieval(String resultId, boolean refresh, int pageSize, int page) {
		return Arrays.asList(
			new String[] {"12", "17.0", "A looooooooooong String"},
			new String[] {"12", "17.0", "A looooooooooong String"},
			new String[] {"12", "17.0", "A looooooooooong String"},
			new String[] {"12", "17.0", "A looooooooooong String"},
			new String[] {"12", "17.0", "A looooooooooong String"},
			new String[] {"12", "17.0", "A looooooooooong String"},
			new String[] {"12", "17.0", "A looooooooooong String"});
	}

	// --------------------------------------------------------------------------------------------

	private String printOrError(Supplier<String> r) {
		try {
			return r.get();
		} catch (SqlExecutionException e) {
			return CliStrings.messageError(CliStrings.MESSAGE_SQL_EXECUTION_ERROR, e);
		}
	}

	// --------------------------------------------------------------------------------------------

	public static class CliResultDescriptor {

		private String resultId;

		private String[] columnNames;

		public CliResultDescriptor(String resultId, String[] columnNames) {
			this.resultId = resultId;
			this.columnNames = columnNames;
		}

		public String getResultId() {
			return resultId;
		}

		public String[] getColumnNames() {
			return columnNames;
		}
	}
}
