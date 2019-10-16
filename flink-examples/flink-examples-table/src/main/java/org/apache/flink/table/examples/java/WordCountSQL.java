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

import org.apache.flink.shaded.asm6.org.objectweb.asm.ClassReader;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

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

	private static ClassReader getClassReader(Class<?> cls) {
		String className = cls.getName().replaceFirst("^.*\\.", "") + ".class";
		try {
			return new ClassReader(cls.getResourceAsStream(className));
		} catch (IOException e) {
			throw new RuntimeException("Could not create ClassReader: " + e.getMessage(), e);
		}
	}

	public static void test(int i, long l) {
		System.out.println(2);
	}

	public static void test(int i, Long l) {
		System.out.println(1);
	}

	public static void test(Integer i) {

	}

	public static void main(String[] args) throws Exception {

		java.sql.Time.valueOf("12:12:12");


//		val settings = EnvironmentSettings.newInstance()
//			.useBlinkPlanner()
//			.inBatchMode()
//			.build();
//
//		val env = TableEnvironment.create(settings);
//
//		env.registerCatalog("enterprise", new HiveCatalog(...))
//
//		env.createTemporaryFunction("normalize", Normalizer.class);
//
//		val customers = env.sqlQuery("SELECT * FROM enterprise.sensitive_data.customers");
//
//		customers.dropColumns(12 to 34).
//
//		env.execute();
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
			long i = frequency % 2;
			Preconditions.checkArgument(frequency > i);
			this.word = Preconditions.checkNotNull(word, "test" + i);
			this.frequency = frequency + i;
		}

		@Override
		public String toString() {
			return "WC " + word + " " + frequency;
		}
	}
}
