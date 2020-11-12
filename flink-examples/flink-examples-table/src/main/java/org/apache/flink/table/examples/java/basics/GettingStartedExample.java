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

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

/**
 * Example for getting started with the Table & SQL API.
 *
 * <p>The example shows how to create, transform, and query a table. It should give a first impression
 * about the look-and-feel of the API without going too much into details. See the other examples for
 * using connectors or more complex operations.
 *
 * <p>In particular, the example shows how to
 * <ul>
 *     <li>setup a {@link TableEnvironment},
 *     <li>use the environment for creating example tables, registering views, and executing SQL queries,
 *     <li>transform tables with filters and projections,
 *     <li>declare user-defined functions,
 *     <li>and print/collect results locally.
 * </ul>
 *
 * <p>The example executes two Flink jobs. The results are written to stdout.
 */
public final class GettingStartedExample {

	public static void main(String[] args) throws Exception {

		// setup the unified API
		// in this case: declare that the table programs should be executed in batch mode
		final EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.inBatchMode()
			.build();
		final TableEnvironment env = TableEnvironment.create(settings);
		env.createTemporaryFunction("func", MyFunc.class);
		env.createTemporaryFunction("MyRow", MyRow.class);

		env.sqlQuery("SELECT func(MyRow(12, 'Hello'))").execute().print();
	}

	public static class MyFunc extends ScalarFunction {
		public String eval(@DataTypeHint("ROW<i INT, s STRING>") Row r) {
			return r.toString();
		}
	}

	public static class MyRow extends ScalarFunction {
		public Row eval(Object... fields) {
			return Row.of(fields);
		}

		@Override
		public TypeInference getTypeInference(DataTypeFactory typeFactory) {
			return BuiltInFunctionDefinitions.ROW.getTypeInference(typeFactory);
		}
	}
}
