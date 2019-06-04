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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simple type inference logic that assumes explicit input and output data types without overloading
 * or variable arguments.
 *
 * <p>For example: {@code (DOUBLE, INT) -> DOUBLE} or {@code (STRING, STRING) -> STRING}
 */
@PublicEvolving
public final class SimpleTypeInference extends TypeInference {

	private final Map<String, DataType> inputTypes;

	private final DataType outputType;

	private SimpleTypeInference(Map<String, DataType> inputTypes, DataType outputType) {
		this.inputTypes = inputTypes;
		this.outputType = outputType;
	}

	/**
	 * Exposes internal structures until {@link TypeInference} is rich enough.
	 */
	@Internal
	public Map<String, DataType> getInputTypes() {
		return inputTypes;
	}

	/**
	 * Exposes internal structures until {@link TypeInference} is rich enough.
	 */
	@Internal
	public DataType getOutputType() {
		return outputType;
	}

	// --------------------------------------------------------------------------------------------

	public static class Builder {

		private final Map<String, DataType> inputTypes;

		private DataType outputType;

		public Builder() {
			// allow a fluent definition even though all parameters are mandatory
			this.inputTypes = new LinkedHashMap<>();
		}

		/**
		 * Adds information about an input parameter. The order in which this method is called determines
		 * the order of parameters.
		 */
		public Builder input(String name, DataType dataType) {
			this.inputTypes.put(
				Preconditions.checkNotNull(name, "Input name must not be null."),
				Preconditions.checkNotNull(dataType, "Input data type must not be null."));
			return this;
		}

		/**
		 * Adds information about the return data type.
		 */
		public Builder output(DataType dataType) {
			this.outputType = Preconditions.checkNotNull(dataType, "Return data type must not be null.");
			return this;
		}

		public SimpleTypeInference build() {
			return new SimpleTypeInference(inputTypes, outputType);
		}
	}
}
