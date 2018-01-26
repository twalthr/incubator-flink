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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.typeutils.TypeStringUtils;
import org.apache.flink.types.Either;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
				resultDesc.getResultSchema().getColumnNames(),
				resultDesc.getResultSchema().getTypes()));
		} catch (SqlExecutionException e) {
			return Either.Right(CliStrings.messageError(CliStrings.MESSAGE_SQL_EXECUTION_ERROR, e));
		}
	}

	/**
	 * Returns either the number of pages or an error message.
	 */
	public Either<Integer, String> translateResultSnapshot(String resultId, int pageSize) {
		try {
			return Either.Left(this.executor.snapshotResult(resultId, pageSize));
		} catch (SqlExecutionException e) {
			return Either.Right(CliStrings.messageError(CliStrings.MESSAGE_SQL_EXECUTION_ERROR, e));
		}
	}

	public Either<List<String[]>, String> translateResultValues(String resultId, int page) {
		try {
			final List<Row> rows = this.executor.retrieveResult(resultId, page);
			if (rows == null) {
				return Either.Right(CliStrings.messageError(CliStrings.MESSAGE_RESULT_TIMEOUT));
			}
			final List<String[]> stringRows = rows
				.stream()
				.map((row) -> {
					final String[] fields = new String[row.getArity()];
					for (int i = 0; i < row.getArity(); i++) {
						fields[i] = row.getField(i).toString();
					}
					return fields;
				})
				.collect(Collectors.toList());
			return Either.Left(stringRows);
		} catch (SqlExecutionException e) {
			return Either.Right(CliStrings.messageError(CliStrings.MESSAGE_SQL_EXECUTION_ERROR, e));
		}
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

		private String[] columnTypes;

		public CliResultDescriptor(String resultId, String[] columnNames, TypeInformation<?>[] columnTypes) {
			this.resultId = resultId;
			this.columnNames = columnNames;
			this.columnTypes = Arrays.stream(columnTypes)
				.map(Object::toString)
				.toArray(String[]::new);
		}

		public String getResultId() {
			return resultId;
		}

		public String[] getColumnNames() {
			return columnNames;
		}

		public String[] getColumnTypes() {
			return columnTypes;
		}
	}
}
