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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Utility for creating a simple {@link TypeInference} that defines all types explicitly and does
 * not support function overloading or varargs.
 */
@PublicEvolving
public final class SimpleTypeInference {

	/**
	 * Returns a builder for creating a simple {@link TypeInference} instance.
	 */
	public static Builder newInstance() {
		return new Builder();
	}

	/**
	 * Builder for creating a simplified {@link TypeInference}.
	 */
	public static class Builder {

		private DataType[] argumentTypes = new DataType[0];

		private String[] argumentNames;

		private DataType accumulatorType;

		private DataType outputType;

		public Builder() {
			// default constructor for fluent definition
		}

		/**
		 * Defines the expected argument types of the function.
		 */
		public Builder argumentTypes(DataType... argumentTypes) {
			this.argumentTypes = Preconditions.checkNotNull(
				argumentTypes,
				"Argument types must not be null.");
			return this;
		}

		/**
		 * Defines the expected argument names of the function for the types defined in
		 * {@link #argumentTypes(DataType...)}.
		 */
		public Builder argumentNames(String... argumentNames) {
			this.argumentNames = Preconditions.checkNotNull(
				argumentNames,
				"Argument names must not be null.");
			return this;
		}

		/**
		 * Defines the accumulator type for aggregating functions that maintain an intermediate
		 * result.
		 */
		public Builder accumulatorType(DataType accumulatorType) {
			this.accumulatorType = Preconditions.checkNotNull(
				accumulatorType,
				"Accumulator type must not be null.");
			return this;
		}

		/**
		 * Defines the output type of a function.
		 */
		public Builder outputType(DataType outputType) {
			this.outputType = Preconditions.checkNotNull(
				outputType,
				"Output type must not be null.");
			return this;
		}

		/**
		 * Returns a simple {@link TypeInference}.
		 */
		public TypeInference build() {
			final TypeInference.Builder builder = TypeInference.newInstance();

			builder.typedArguments(Arrays.asList(argumentTypes));

			if (argumentNames != null) {
				builder.namedArguments(Arrays.asList(argumentNames));
			}

			builder.inputTypeValidator(new SimpleInputTypeValidator(argumentTypes, argumentNames));

			if (accumulatorType != null) {
				builder.accumulatorTypeStrategy(new SimpleTypeStrategy(accumulatorType));
			}

			if (outputType != null) {
				builder.outputTypeStrategy(new SimpleTypeStrategy(outputType));
			}

			return builder.build();
		}
	}

	// --------------------------------------------------------------------------------------------

	private static class SimpleInputTypeValidator implements InputTypeValidator {

		private final DataType[] expectedTypes;

		private final String[] expectedNames;

		public SimpleInputTypeValidator(DataType[] argumentTypes, String[] expectedNames) {
			this.expectedTypes = argumentTypes;
			this.expectedNames = expectedNames;
		}

		@Override
		public ArgumentCount getArgumentCount() {
			return new SimpleArgumentCount(expectedTypes.length);
		}

		@Override
		public boolean validate(CallContext callContext, boolean throwOnFailure) {
			final List<DataType> actualTypes = callContext.getArgumentDataTypes();

			for (int i = 0; i < expectedTypes.length; i++) {
				final LogicalType expectedType = expectedTypes[i].getLogicalType();
				final LogicalType actualType = actualTypes.get(i).getLogicalType();
				if (!LogicalTypeCasts.supportsImplicitCast(actualType, expectedType)) {
					if (throwOnFailure) {
						throw new ValidationException(
							String.format(
								"Invalid argument type. Expected type: [%s]. Actual type: [%s].",
								expectedType,
								actualType
							)
						);
					}
					return false;
				}
			}
			return true;
		}

		@Override
		public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
			final List<Signature.Argument> arguments = new ArrayList<>();
			for (int i = 0; i < expectedTypes.length; i++) {
				if (expectedNames != null) {
					arguments.add(Signature.Argument.of(expectedNames[i], expectedTypes[i].toString()));
				} else {
					arguments.add(Signature.Argument.of(expectedTypes[i].toString()));
				}
			}
			return Collections.singletonList(Signature.of(arguments));
		}
	}

	private static class SimpleArgumentCount implements ArgumentCount {

		private final int length;

		public SimpleArgumentCount(int length) {
			this.length = length;
		}

		@Override
		public boolean isValidCount(int count) {
			return count == length;
		}

		@Override
		public Optional<Integer> getMinCount() {
			return Optional.of(length);
		}

		@Override
		public Optional<Integer> getMaxCount() {
			return Optional.of(length);
		}
	}

	private static class SimpleTypeStrategy implements TypeStrategy {

		private final DataType type;

		public SimpleTypeStrategy(DataType type) {
			this.type = type;
		}

		@Override
		public Optional<DataType> inferType(CallContext callContext) {
			return Optional.of(type);
		}
	}

	private SimpleTypeInference() {
		// no instantiation
	}
}
