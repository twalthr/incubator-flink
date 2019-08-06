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

package org.apache.flink.table.types;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.utils.ReflectiveDataTypeConverter;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ReflectiveDataTypeConverter}.
 */
@RunWith(Parameterized.class)
public class ReflectiveDataTypeConverterTest {

	@Parameters
	public static List<TestSpec> testData() {
		return Arrays.asList(

			TestSpec
				.forClass(Integer.class)
				.expectDataType(DataTypes.INT()),

			TestSpec
				.forClass(BigDecimal.class)
				.expectErrorMessage(""),

			TestSpec
				.forClass(BigDecimal.class)
				.configuration(b -> b.defaultDecimal(12, 4))
				.expectDataType(DataTypes.DECIMAL(12, 4))
		);
	}

	@Parameter
	public TestSpec testSpec;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testClassExtraction() {
		if (testSpec.argument == TestSpec.NO_ARGUMENT && testSpec.expectedDataType != null) {
			assertThat(
				getConfiguredExtractor().extractDataType(testSpec.clazz),
				equalTo(testSpec.expectedDataType));
		}
	}

	@Test
	public void testArgumentExtraction() {
		if (testSpec.argument != TestSpec.NO_ARGUMENT && testSpec.expectedDataType != null) {
			assertThat(
				getConfiguredExtractor().extractDataType(testSpec.clazz, testSpec.argument),
				equalTo(testSpec.expectedDataType));
		}
	}

	@Test
	public void testClassExtractionError() {
		if (testSpec.argument == TestSpec.NO_ARGUMENT && testSpec.expectedErrorMessage != null) {
			thrown.expect(ValidationException.class);
			thrown.expectMessage(testSpec.expectedErrorMessage);
			getConfiguredExtractor().extractDataType(testSpec.clazz);
		}
	}

	@Test
	public void testArgumentExtractionError() {
		if (testSpec.argument != TestSpec.NO_ARGUMENT && testSpec.expectedErrorMessage != null) {
			thrown.expect(ValidationException.class);
			thrown.expectMessage(testSpec.expectedErrorMessage);
			getConfiguredExtractor().extractDataType(testSpec.clazz, testSpec.argument);
		}
	}

	// --------------------------------------------------------------------------------------------

	private ReflectiveDataTypeConverter getConfiguredExtractor() {
		final ReflectiveDataTypeConverter.Builder builder = ReflectiveDataTypeConverter.newInstance();
		if (testSpec.configuration != null) {
			testSpec.configuration.accept(builder);
		}
		return builder.build();
	}

	private static class TestSpec {

		static final int NO_ARGUMENT = -1;

		final Class<?> clazz;

		final int argument;

		@Nullable Consumer<ReflectiveDataTypeConverter.Builder> configuration;

		@Nullable DataType expectedDataType;

		@Nullable String expectedErrorMessage;

		TestSpec(Class<?> clazz, int argument) {
			this.clazz = clazz;
			this.argument = argument;
		}

		static TestSpec forClass(Class<?> clazz) {
			return new TestSpec(clazz, NO_ARGUMENT);
		}

		static TestSpec forArgumentOf(Class<?> clazz, int argument) {
			return new TestSpec(clazz, argument);
		}

		TestSpec configuration(Consumer<ReflectiveDataTypeConverter.Builder> configuration) {
			this.configuration = configuration;
			return this;
		}

		TestSpec expectDataType(DataType expectedDataType) {
			this.expectedDataType = expectedDataType;
			return this;
		}

		TestSpec expectErrorMessage(String expectedErrorMessage) {
			this.expectedErrorMessage = expectedErrorMessage;
			return this;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Test classes for extraction
	// --------------------------------------------------------------------------------------------

//	public static class SimpleStructured
}
