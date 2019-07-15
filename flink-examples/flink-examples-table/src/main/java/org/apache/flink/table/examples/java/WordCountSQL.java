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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.shaded.asm6.org.objectweb.asm.ClassReader;
import org.apache.flink.shaded.asm6.org.objectweb.asm.ClassVisitor;
import org.apache.flink.shaded.asm6.org.objectweb.asm.Label;
import org.apache.flink.shaded.asm6.org.objectweb.asm.MethodVisitor;
import org.apache.flink.shaded.asm6.org.objectweb.asm.Opcodes;
import org.apache.flink.shaded.asm6.org.objectweb.asm.Type;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;

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

	public static void main(String[] args) throws Exception {

		String s = Type.getConstructorDescriptor(WordCountSQL.WC.class.getDeclaredConstructors()[1]);

		ArrayList<String> fields = new ArrayList<>();

		getClassReader(WordCountSQL.WC.class).accept(new ClassVisitor(Opcodes.ASM6) {

			@Override
			public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
				return new MethodVisitor(Opcodes.ASM6) {
					@Override
					public void visitLocalVariable(String name, String descriptor, String signature, Label start, Label end, int index) {
						super.visitLocalVariable(name, descriptor, signature, start, end, index);
					}
				};
			}
		}, 0);

		System.out.println();

//		MethodHandle handle = MethodHandles.lookup().findConstructor(
//			org.apache.flink.table.examples.scala.WordCountSQL.WC.class,
//			MethodType.methodType(void.class, String.class, long.class));
//
//		handle.bindTo()



		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

		DataSet<WC> input = env.fromElements(
			new WC("Hello", 1),
			new WC("Ciao", 1),
			new WC("Hello", 1));

		Class c = org.apache.flink.table.examples.scala.WordCountSQL.WC.class;

		Class c2 = Class.forName("org.apache.flink.table.examples.scala.WordCountSQL$WC$");

		org.apache.flink.table.examples.scala.WordCountSQL.WC wc = new org.apache.flink.table.examples.scala.WordCountSQL.WC("", 12);

		// register the DataSet as table "WordCount"
		tEnv.registerDataSet("WordCount", input, "word, frequency");

		// run a SQL query on the Table and retrieve the result as a new Table
		Table table = tEnv.sqlQuery(
			"SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word");

		DataSet<WC> result = tEnv.toDataSet(table, WC.class);

		result.print();
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
