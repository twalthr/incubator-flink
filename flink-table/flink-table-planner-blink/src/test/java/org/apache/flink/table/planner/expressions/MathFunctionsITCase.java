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

package org.apache.flink.table.planner.expressions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Tests for math {@link BuiltInFunctionDefinitions} that fully use the new type system.
 */
public class MathFunctionsITCase extends BuiltInFunctionTestBase {

	// TODO add real updated functions of FLIP-51 here

	@Parameters(name = "{index}: {0}")
	public static List<TestSpec> testData() {
		return Arrays.asList(
			TestSpec
				.forFunction(BuiltInFunctionDefinitions.PLUS)
				.onFieldsWithData(123, 456)
				.testTableApiResult($("f0").plus($("f1")), 579, DataTypes.INT()) // should be INT NOT NULL
				.testTableApiError(
					$("f0").plus(true),
					"The arithmetic '('f0 + true)' requires input that is numeric, string, time intervals " +
						"of the same type, or a time interval and a time point type, but was ''f0' : 'Integer' " +
						"and 'true' : 'Boolean'.")
				.testSqlResult("f0 + f1", 579, DataTypes.INT().notNull())
				.testSqlError("f0 + TRUE", "Cannot apply '+' to arguments")
		);
	}
}
