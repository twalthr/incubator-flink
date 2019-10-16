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

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.types.annotations.DataTypeHint;
import org.apache.flink.table.types.annotations.FunctionHint;

/**
 * Simple example for demonstrating the use of the Table API for a Word Count in Java.
 *
 * <p>This example shows how to:
 *  - Convert DataSets to Tables
 *  - Apply group, aggregate, select, and filter operations
 */
public class WordCountTable {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************



	public static class TableFunction<String> {
		@FunctionHint(input = {@DataTypeHint("INT"), @DataTypeHint("BOOLEAN")})
		@FunctionHint(input = {@DataTypeHint("STRING")})
		public void eval(Object... args) {
			///...
		}
	}

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

//		tEnv.registerFunction("tf", new HiveWrapper(HiveUDF.class));
//
//
//		vvv(12, true);
//
//		DataSet<WC> input = env.fromElements(
//				new WC("Hello", 1),
//				new WC("Ciao", 1),
//				new WC("Hello", 1));
//
//		Table table = tEnv.fromDataSet(input);
//
//		Table filtered = table
//				.groupBy("word")
//				.select("word, frequency.sum as frequency")
//				.filter("frequency = 2");
//
//		DataSet<WC> result = tEnv.toDataSet(filtered, WC.class);
//
//		result.print();
	}

//	// function level
//	@FunctionHint(
//		input = {
//			@DataTypeHint("INT"),
//			@DataTypeHint(extract = true, context = @ExtractionContext(
//				version = 1,
//				decimalPrecision = 12,
//				anyPatterns = {"org.joda."}
//			)),
//			@DataTypeHint(value = "Test", bridgedTo = BigDecimal.class)
//		},
//		accumulator = @DataTypeHint(value = "Test", bridgedTo = BigDecimal.class),
//		output = @DataTypeHint(value = "DECIMAL(2, 3)", bridgedTo = BigDecimal.class)
//	)
//	public static class MyFunc extends TableFunction<Object> {
//
//		// method level
//		@FunctionHint(
//			input = @DataTypeHint(value = "TIMESTAMP(3)", bridgedTo = Timestamp.class),
//			output = @DataTypeHint(value = "INT")
//		)
//		public void eval(Object... args) {
//			// ...
//		}
//
//		// method level
//		@FunctionHint(
//			input = @DataTypeHint(value = "INT")
//		)
//		public void eval(int i, long l, @DataTypeHint("ROW<INT a, BOOLEAN b>") Row row) {
//			// ...
//		}
//
//		// parameter level
//		public void eval(int f, double d, @DataTypeHint("ROW(INT a)") Row arg) {
//			// ...
//		}
//
//		// structured type level
//		static class T {
//
//			int field1;
//
//			@DataTypeHint("MAP<INT, STRING>")
//			Object field;
//		}
//	}

	// *************************************************************************
	//     USER DATA TYPES
	// *************************************************************************

	/**
	 * Simple POJO containing a word and its respective count.
	 */
	public static class WC {
		public String word;
		public long frequency;

		// public constructor to make it a Flink POJO
		public WC() {}

		public WC(String word, long frequency) {
			this.word = word;
			this.frequency = frequency;
		}

		@Override
		public String toString() {
			return "WC " + word + " " + frequency;
		}
	}


	public static void vvv(long l, Object... e) {

	}
}
