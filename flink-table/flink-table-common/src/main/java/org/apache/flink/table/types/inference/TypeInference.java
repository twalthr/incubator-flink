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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * Provides logic for inferring and/or validating input and output types of functions.
 *
 * <p>Please note that this class is a stub for now. In the future, it will be replaced by an advanced
 * type inference logic (see FLIP-37, part 2).
 */
@Internal
public class TypeInference {

	private final InputTypeValidator inputTypeValidator;

	private final TypeStrategy accumulatorTypeStrategy;

	private final TypeStrategy outputTypeStrategy;

	private TypeInference(
			InputTypeValidator inputTypeValidator,
			TypeStrategy accumulatorTypeStrategy,
			TypeStrategy outputTypeStrategy) {
		this.inputTypeValidator =
			Preconditions.checkNotNull(inputTypeValidator, "Input type validator must not be null.");
		this.accumulatorTypeStrategy =
			Preconditions.checkNotNull(outputTypeStrategy, "Accumulator type strategy must not be null.");
		this.outputTypeStrategy =
			Preconditions.checkNotNull(outputTypeStrategy, "Output type strategy must not be null.");
	}

	/**
	 * Returns the validator for checking the input data types of a function call.
	 */
	public InputTypeValidator getInputTypeValidator() {
		return inputTypeValidator;
	}

	/**
	 * Returns the strategy for inferring an accumulator data type of a function call.
	 */
	public TypeStrategy getAccumulatorTypeStrategy() {
		return accumulatorTypeStrategy;
	}

	/**
	 * Returns the strategy for inferring an output data type of a function call.
	 */
	public TypeStrategy getOutputTypeStrategy() {
		return outputTypeStrategy;
	}

	// will be filled with implementation as part of FLIP-37, part 2

	// --------------------------------------------------------------------------------------------

	public static class Builder {

		private @Nullable InputTypeValidator inputTypeValidator;

		private @Nullable TypeStrategy accumulatorTypeStrategy;

		private @Nullable TypeStrategy outputTypeStrategy;

		public Builder() {
			// default constructor to allow a fluent definition
		}

		public Builder inputTypeValidator(InputTypeValidator inputTypeValidator) {
			this.inputTypeValidator =
				Preconditions.checkNotNull(inputTypeValidator, "Input type validator must not be null.");
			return this;
		}

		public Builder accumulatorTypeStrategy(TypeStrategy accumulatorTypeStrategy) {
			this.accumulatorTypeStrategy =
				Preconditions.checkNotNull(accumulatorTypeStrategy, "Accumulator type strategy must not be null.");
			return this;
		}

		public Builder outputTypeStrategy(TypeStrategy outputTypeStrategy) {
			this.outputTypeStrategy =
				Preconditions.checkNotNull(outputTypeStrategy, "Output type strategy must not be null.");
			return this;
		}

		public TypeInference build() {
			return new TypeInference(
				inputTypeValidator == null ? InputTypeValidators.NILADIC : inputTypeValidator,
				accumulatorTypeStrategy == null ? TypeStrategies.MISSING : accumulatorTypeStrategy,
				outputTypeStrategy == null ? TypeStrategies.MISSING : outputTypeStrategy);
		}
	}
}
