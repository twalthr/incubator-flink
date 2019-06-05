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

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Simple type inference logic that assumes explicit input, accumulator, and output data types
 * without overloading or variable arguments.
 *
 * <p>For example: {@code (DOUBLE, INT) -> DOUBLE} or {@code (STRING, STRING) -> STRING}
 */
@PublicEvolving
public final class ExplicitTypeInference extends AccumulatingTypeInference {

	private final Map<String, DataType> inputDataTypes;

	private final @Nullable DataType accumulatorDataType;

	private final DataType outputDataType;

	private ExplicitTypeInference(
			Map<String, DataType> inputDataTypes,
			DataType accumulatorDataType,
			DataType outputDataType) {
		this.inputDataTypes = inputDataTypes;
		this.accumulatorDataType = accumulatorDataType;
		this.outputDataType = Preconditions.checkNotNull(outputDataType, "Output data type must not be null.");
	}

	/**
	 * Exposes internal structures until {@link TypeInference} is rich enough.
	 */
	@Internal
	public Map<String, DataType> getInputDataTypes() {
		return inputDataTypes;
	}

	/**
	 * Exposes internal structures until {@link AccumulatingTypeInference} is rich enough.
	 */
	@Internal
	public Optional<DataType> getAccumulatorDataType() {
		return Optional.ofNullable(accumulatorDataType);
	}

	/**
	 * Exposes internal structures until {@link TypeInference} is rich enough.
	 */
	@Internal
	public DataType getOutputDataType() {
		return outputDataType;
	}

	// --------------------------------------------------------------------------------------------

	public static class Builder {

		private final Map<String, DataType> inputDataTypes;

		private DataType accumulatorDataType;

		private DataType outputDataType;

		public Builder() {
			// default constructor to allow a fluent definition
			this.inputDataTypes = new LinkedHashMap<>();
		}

		/**
		 * Adds information about an input parameter. The order in which this method is called determines
		 * the order of parameters. Not required if function does not take parameters.
		 */
		public Builder input(String name, DataType dataType) {
			this.inputDataTypes.put(
				Preconditions.checkNotNull(name, "Input name must not be null."),
				Preconditions.checkNotNull(dataType, "Input data type must not be null."));
			return this;
		}

		/**
		 * Adds a data type for the intermediate result data type. Optional.
		 */
		public Builder accumulator(DataType dataType) {
			this.accumulatorDataType = Preconditions.checkNotNull(dataType, "Accumulator data type must not be null.");
			return this;
		}

		/**
		 * Adds a data type for the result. Mandatory.
		 */
		public Builder output(DataType dataType) {
			this.outputDataType = Preconditions.checkNotNull(dataType, "Output data type must not be null.");
			return this;
		}

		public ExplicitTypeInference build() {
			return new ExplicitTypeInference(inputDataTypes, accumulatorDataType, outputDataType);
		}
	}
}
