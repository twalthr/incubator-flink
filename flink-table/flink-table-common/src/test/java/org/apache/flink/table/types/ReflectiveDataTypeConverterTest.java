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

import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.utils.ReflectiveDataTypeConverter;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import javax.annotation.Nullable;

import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ReflectiveDataTypeConverter}.
 */
@RunWith(Parameterized.class)
public class ReflectiveDataTypeConverterTest {

	@Test
	public void testCalaa() {
//		final ReflectiveDataTypeConverter converter = ReflectiveDataTypeConverter.newInstance().build();
//
//		final ParameterizedType type = (ParameterizedType) TestFunction.class.getGenericSuperclass();
//
//		converter.createTypeVariableMapping(type);
	}

	@Parameter
	public TestSpec testSpec;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testClassExtraction() {
		if (testSpec.clazz != null && testSpec.expectedDataType != null) {
			assertThat(
				getConfiguredExtractor().extractDataType(testSpec.clazz),
				equalTo(testSpec.expectedDataType));
		}
	}

	@Test
	public void testArgumentExtraction() {
		if (testSpec.function != null && testSpec.expectedDataType != null) {
			assertThat(
				getConfiguredExtractor().extractDataType(testSpec.clazz),
				equalTo(testSpec.expectedDataType));
		}
	}

	private ReflectiveDataTypeConverter getConfiguredExtractor() {
		final ReflectiveDataTypeConverter.Builder builder = ReflectiveDataTypeConverter.newInstance();
		if (testSpec.configuration != null) {
			testSpec.configuration.accept(builder);
		}
		return builder.build();
	}

	// --------------------------------------------------------------------------------------------

	private static class TestSpec {

		final @Nullable Class<?> clazz;

		final @Nullable Cl function;

		@Nullable Consumer<ReflectiveDataTypeConverter.Builder> configuration;

		@Nullable DataType expectedDataType;

		@Nullable String expectedErrorMessage;

		TestSpec(Class<?> clazz) {
			this.clazz = clazz;
			this.function = null;
		}

		TestSpec(TableFunction<?> function) {
			this.clazz = null;
			this.function = function;
		}

		static TestSpec forClass(Class<?> clazz) {
			return new TestSpec(clazz);
		}

		static TestSpec forArgumentOf(TableFunction<?> function) {
			return new TestSpec(function);
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

	public static class SimpleStructured
}
