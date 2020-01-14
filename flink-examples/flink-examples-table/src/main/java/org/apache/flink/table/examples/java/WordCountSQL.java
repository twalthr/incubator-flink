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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * Simple example that shows how the Batch SQL API is used in Java.
 *
 * <p>This example shows how to:
 *  - Convert DataSets to Tables
 *  - Register a Table under a name
 *  - Run a SQL query on the registered Table
 */
public class WordCountSQL {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// set up execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(
			env,
			EnvironmentSettings.newInstance().useBlinkPlanner().build());

		tEnv.createTemporarySystemFunction("testi", new ScalarFunction() {
			public String eval(int i) {
				return String.valueOf(i);
			}
		});

		DataStream<WC> input = env.fromElements(
			new WC("Hello", 1),
			new WC("Ciao", 1),
			new WC("Hello", 1));

		// register the DataSet as a view "WordCount"
		tEnv.createTemporaryView("WordCount", input, "word, frequency");

		// run a SQL query on the Table and retrieve the result as a new Table
		Table table = tEnv.sqlQuery(
			"SELECT testi(12) FROM WordCount");

		tEnv.toRetractStream(table, Row.class).print();
	}

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
}
