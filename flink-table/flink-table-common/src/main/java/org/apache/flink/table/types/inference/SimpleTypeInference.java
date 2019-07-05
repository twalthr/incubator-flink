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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

/**
 * Utility for creating a simple {@link TypeInference} that defines all types explicitly and does
 * not support function overloading.
 */
@PublicEvolving
public final class SimpleTypeInference {

	public static Builder newInstance() {
		return new Builder();
	}

	/**
	 * Builder for creating a simplified {@link TypeInference}.
	 */
	public static class Builder {

		private DataType[] argumentTypes;

		private String[] argumentNames;

		private boolean acceptsVarArgs;

		private DataType accumulatorType;

		private DataType outputType;

		public Builder() {
			// default constructor for fluent definition
		}

		/**
		 * Defines the expected argument types. Required.
		 */
		public Builder argumentTypes(DataType... argumentTypes) {
			this.argumentTypes = Preconditions.checkNotNull(
				argumentTypes,
				"Argument types must not be null.");
			return this;
		}

		/**
		 * Defines the expected argument names for the type.
		 */
		public Builder argumentNames(String... argumentNames) {
			this.argumentNames = Preconditions.checkNotNull(
				argumentNames,
				"Argument names must not be null.");
			return this;
		}

		/**
		 * Declares that the function accepts a variable number of arguments.
		 *
		 * <p>In other words: The last argument type specified in {@link #argumentTypes(DataType...)}
		 * can occur multiple times.
		 */
		public Builder acceptsVarArgs() {
			this.acceptsVarArgs = true;
			return this;
		}

		public Builder accumulatorType(DataType accumulatorType) {
			this.accumulatorType = Preconditions.checkNotNull(
				accumulatorType,
				"Accumulator type must not be null.");
			return this;
		}

		public Builder outputType(DataType outputType) {

		}
	}

	private SimpleTypeInference() {
		// no instantiation
	}
}
