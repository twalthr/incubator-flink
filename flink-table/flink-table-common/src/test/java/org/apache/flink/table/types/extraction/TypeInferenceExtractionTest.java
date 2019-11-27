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

package org.apache.flink.table.types.extraction;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeLookup;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeValidator;
import org.apache.flink.table.types.inference.InputTypeValidator;
import org.apache.flink.table.types.inference.InputTypeValidators;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.CoreMatchers.containsCause;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link TypeInferenceExtractor}.
 */
@RunWith(Parameterized.class)
@SuppressWarnings("unused")
public class TypeInferenceExtractionTest {

	@Parameters
	public static List<TestSpec> testData() {
		return Arrays.asList(
			TestSpec
				.forFunction(FullFunctionHint.class)
				.expectOutputMapping(
					InputTypeValidators.sequence(
						new String[] {"i", "s"},
						new ArgumentTypeValidator[] {
							InputTypeValidators.explicit(DataTypes.INT()),
							InputTypeValidators.explicit(DataTypes.STRING())}
					),
					TypeStrategies.explicit(DataTypes.BOOLEAN()))
				.expectArgumentTypes(DataTypes.INT(), DataTypes.STRING())
				.expectArgumentNames("i", "s"),

			TestSpec
				.forFunction(FullFunctionHints.class)
				.expectOutputMapping(
					InputTypeValidators.sequence(InputTypeValidators.explicit(DataTypes.INT())),
					TypeStrategies.explicit(DataTypes.INT()))
				.expectOutputMapping(
					InputTypeValidators.sequence(InputTypeValidators.explicit(DataTypes.BIGINT())),
					TypeStrategies.explicit(DataTypes.BIGINT())),

			TestSpec
				.forFunction(GlobalOutputFunctionHint.class)
				.expectOutputMapping(
					InputTypeValidators.sequence(InputTypeValidators.explicit(DataTypes.INT())),
					TypeStrategies.explicit(DataTypes.INT()))
				.expectOutputMapping(
					InputTypeValidators.sequence(InputTypeValidators.explicit(DataTypes.STRING())),
					TypeStrategies.explicit(DataTypes.INT())),

			TestSpec
				.forFunction(InvalidSingleOutputFunctionHint.class)
				.expectErrorMessage("Function hints with same input definition but different output types are not allowed."),

			TestSpec
				.forFunction(SplitFullFunctionHints.class)
				.expectOutputMapping(
					InputTypeValidators.sequence(InputTypeValidators.explicit(DataTypes.INT())),
					TypeStrategies.explicit(DataTypes.INT()))
				.expectOutputMapping(
					InputTypeValidators.sequence(InputTypeValidators.explicit(DataTypes.BIGINT())),
					TypeStrategies.explicit(DataTypes.BIGINT())),

			TestSpec
				.forFunction(InvalidFullOutputFunctionHint.class)
				.expectErrorMessage("Function hints with same input definition but different output types are not allowed."),

			TestSpec
				.forFunction(IncompleteFunctionHint.class)
				.expectErrorMessage("Data type hint does neither specify an explicit data type nor an input group."),

			TestSpec
				.forFunction(ComplexFunctionHint.class)
				.expectOutputMapping(
					InputTypeValidators.varyingSequence(
						new String[]{"myInt", "myAny"},
						new ArgumentTypeValidator[]{InputTypeValidators.explicit(DataTypes.INT()), InputTypeValidators.ANY}),
					TypeStrategies.explicit(DataTypes.BOOLEAN())),

			TestSpec
				.forFunction(ComplexFunctionHint.class)
				.expectOutputMapping(
					InputTypeValidators.varyingSequence(
						new String[]{"myInt", "myAny"},
						new ArgumentTypeValidator[]{InputTypeValidators.explicit(DataTypes.INT()), InputTypeValidators.ANY}),
					TypeStrategies.explicit(DataTypes.BOOLEAN())),

			TestSpec
				.forFunction(InvalidOutputWithArgNamesFunctionHint.class)
				.expectErrorMessage("Function hints with same input definition but different output types are not allowed."),

			TestSpec
				.forFunction(GlobalInputFunctionHints.class)
				.expectOutputMapping(
					InputTypeValidators.sequence(InputTypeValidators.explicit(DataTypes.INT())),
					TypeStrategies.explicit(DataTypes.INT()))
				.expectOutputMapping(
					InputTypeValidators.sequence(InputTypeValidators.explicit(DataTypes.BIGINT())),
					TypeStrategies.explicit(DataTypes.INT()))
		);
	}

	@Parameter
	public TestSpec testSpec;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testArgumentNames() {
		if (testSpec.expectedArgumentNames != null) {
			assertThat(
				inferTypes().getArgumentNames(),
				equalTo(Optional.of(testSpec.expectedArgumentNames)));
		}
	}

	@Test
	public void testArgumentTypes() {
		if (testSpec.expectedArgumentTypes != null) {
			assertThat(
				inferTypes().getArgumentTypes(),
				equalTo(Optional.of(testSpec.expectedArgumentTypes)));
		}
	}

	@Test
	public void testAccumulatorTypeStrategy() {
		if (!testSpec.expectedAccumulatorStrategies.isEmpty()) {
			assertThat(
				inferTypes().getAccumulatorTypeStrategy().isPresent(),
				equalTo(true));
			assertThat(
				inferTypes().getAccumulatorTypeStrategy().get(),
				equalTo(TypeStrategies.matching(testSpec.expectedAccumulatorStrategies)));
		}
	}

	@Test
	public void testOutputTypeStrategy() {
		if (!testSpec.expectedOutputStrategies.isEmpty()) {
			assertThat(
				inferTypes().getOutputTypeStrategy(),
				equalTo(TypeStrategies.matching(testSpec.expectedOutputStrategies)));
		}
	}

	@Test
	public void testErrorMessage() {
		if (testSpec.expectedErrorMessage != null) {
			thrown.expect(ValidationException.class);
			thrown.expectCause(containsCause(new ValidationException(testSpec.expectedErrorMessage)));
			inferTypes();
		}
	}

	// --------------------------------------------------------------------------------------------
	// Test utilities
	// --------------------------------------------------------------------------------------------

	private static class TestSpec {

		final Class<? extends UserDefinedFunction> function;

		@Nullable List<String> expectedArgumentNames;

		@Nullable List<DataType> expectedArgumentTypes;

		Map<InputTypeValidator, TypeStrategy> expectedAccumulatorStrategies;

		Map<InputTypeValidator, TypeStrategy> expectedOutputStrategies;

		@Nullable String expectedErrorMessage;

		private TestSpec(Class<? extends UserDefinedFunction> function) {
			this.function = function;
			this.expectedAccumulatorStrategies = new HashMap<>();
			this.expectedOutputStrategies = new HashMap<>();
		}

		static TestSpec forFunction(Class<? extends UserDefinedFunction> function) {
			return new TestSpec(function);
		}

		TestSpec expectArgumentNames(String... expectedArgumentNames) {
			this.expectedArgumentNames = Arrays.asList(expectedArgumentNames);
			return this;
		}

		TestSpec expectArgumentTypes(DataType... expectedArgumentTypes) {
			this.expectedArgumentTypes = Arrays.asList(expectedArgumentTypes);
			return this;
		}

		TestSpec expectAccumulatorMapping(InputTypeValidator validator, TypeStrategy accumulatorStrategy) {
			this.expectedAccumulatorStrategies.put(validator, accumulatorStrategy);
			return this;
		}

		TestSpec expectOutputMapping(InputTypeValidator validator, TypeStrategy outputStrategy) {
			this.expectedOutputStrategies.put(validator, outputStrategy);
			return this;
		}

		TestSpec expectErrorMessage(String expectedErrorMessage) {
			this.expectedErrorMessage = expectedErrorMessage;
			return this;
		}
	}

	@SuppressWarnings("unchecked")
	private TypeInference inferTypes() {
		if (ScalarFunction.class.isAssignableFrom(testSpec.function)) {
			return TypeInferenceExtractor.extractFromScalarFunction(new DataTypeLookupMock(), (Class) testSpec.function);
		} else if (TableFunction.class.isAssignableFrom(testSpec.function)) {
			return TypeInferenceExtractor.extractFromTableFunction((Class) testSpec.function);
		} else if (AggregateFunction.class.isAssignableFrom(testSpec.function)) {
			return TypeInferenceExtractor.extractFromAggregateFunction((Class) testSpec.function);
		} else if (TableAggregateFunction.class.isAssignableFrom(testSpec.function)) {
			return TypeInferenceExtractor.extractFromTableAggregateFunction((Class) testSpec.function);
		} else if (AsyncTableFunction.class.isAssignableFrom(testSpec.function)) {
			return TypeInferenceExtractor.extractFromAsyncTableFunction((Class) testSpec.function);
		}
		throw new AssertionError("Unsupported function.");
	}

	private static class DataTypeLookupMock implements DataTypeLookup {

		@Override
		public Optional<DataType> lookupDataType(String name) {
			return Optional.of(TypeConversions.fromLogicalToDataType(LogicalTypeParser.parse(name)));
		}

		@Override
		public Optional<DataType> lookupDataType(UnresolvedIdentifier identifier) {
			return Optional.empty();
		}

		@Override
		public DataType resolveRawDataType(Class<?> clazz) {
			return null;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Test classes for extraction
	// --------------------------------------------------------------------------------------------

	@FunctionHint(
		input = {@DataTypeHint("INT"), @DataTypeHint("STRING")},
		argumentNames = {"i", "s"},
		output = @DataTypeHint("BOOLEAN")
	)
	public static class FullFunctionHint extends ScalarFunction {
		public Boolean eval(Integer i, String s) {
			return null;
		}
	}

	public static class ComplexFunctionHint extends ScalarFunction {
		@FunctionHint(
			input = {@DataTypeHint("INT"), @DataTypeHint(inputGroup = InputGroup.ANY)},
			argumentNames = {"myInt", "myAny"},
			output = @DataTypeHint("BOOLEAN"),
			isVarArgs = true
		)
		public Boolean eval(Object... o) {
			return null;
		}
	}

	@FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
	@FunctionHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("BIGINT"))
	public static class FullFunctionHints extends ScalarFunction {
		public Number eval(Number n) {
			return null;
		}
	}

	@FunctionHint(output = @DataTypeHint("INT"))
	public static class GlobalOutputFunctionHint extends ScalarFunction {
		@FunctionHint(input = @DataTypeHint("INT"))
		public Integer eval(Integer n) {
			return null;
		}
		@FunctionHint(input = @DataTypeHint("STRING"))
		public Integer eval(String n) {
			return null;
		}
	}

	@FunctionHint(output = @DataTypeHint("INT"))
	public static class InvalidSingleOutputFunctionHint extends ScalarFunction {
		@FunctionHint(output = @DataTypeHint("STRING"))
		public Integer eval(Integer n) {
			return null;
		}
	}

	@FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
	public static class SplitFullFunctionHints extends ScalarFunction {
		@FunctionHint(input = @DataTypeHint("BIGINT"), output = @DataTypeHint("BIGINT"))
		public Number eval(Number n) {
			return null;
		}
	}

	@FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("INT"))
	public static class InvalidFullOutputFunctionHint extends ScalarFunction {
		@FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("BIGINT"))
		public Boolean eval(Integer i) {
			return null;
		}
	}

	@FunctionHint(input = @DataTypeHint("INT"))
	public static class InvalidLocalOutputFunctionHint extends ScalarFunction {
		@FunctionHint(output = @DataTypeHint("INT"))
		public Integer eval(Integer n) {
			return null;
		}
		@FunctionHint(output = @DataTypeHint("STRING"))
		public Integer eval(String n) {
			return null;
		}
	}

	@FunctionHint(input = {@DataTypeHint("INT"), @DataTypeHint()}, output = @DataTypeHint("BOOLEAN"))
	public static class IncompleteFunctionHint extends ScalarFunction {
		public Boolean eval(Integer i1, Integer i2) {
			return null;
		}
	}

	@FunctionHint(
		input = @DataTypeHint("INT"),
		argumentNames = "a",
		output = @DataTypeHint("BOOLEAN"))
	public static class InvalidOutputWithArgNamesFunctionHint extends ScalarFunction {
		@FunctionHint(
			input = @DataTypeHint("INT"),
			argumentNames = "b",
			output = @DataTypeHint("INT"))
		public Boolean eval(Integer i) {
			return null;
		}
	}

	@FunctionHint(input = @DataTypeHint("INT"))
	@FunctionHint(input = @DataTypeHint("BIGINT"))
	public static class GlobalInputFunctionHints extends ScalarFunction {
		@FunctionHint(output = @DataTypeHint("INT"))
		public Integer eval(Number n) {
			return null;
		}
	}
}
