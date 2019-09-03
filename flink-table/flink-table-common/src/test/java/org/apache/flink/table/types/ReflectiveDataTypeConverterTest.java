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

import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TypeInformationAnyType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.ReflectiveDataTypeConverter;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nullable;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ReflectiveDataTypeConverter}.
 */
@RunWith(Parameterized.class)
public class ReflectiveDataTypeConverterTest {

	@Parameters
	public static List<TestSpec> testData() {
		return Arrays.asList(

//			TestSpec
//				.forType(Integer.class)
//				.expectDataType(DataTypes.INT()),
//
//			TestSpec
//				.forType(Integer.class)
//				.configuration(b -> b.anyPatterns(Collections.singletonList("java.lang.")))
//				.expectDataType(DataTypes.ANY(new GenericTypeInfo<>(Integer.class))),
//
//			TestSpec
//				.forType(BigDecimal.class)
//				.expectErrorMessage("need fixed precision and scale"),
//
//			TestSpec
//				.forType(BigDecimal.class)
//				.configuration(b -> b.defaultDecimal(12, 4))
//				.expectDataType(DataTypes.DECIMAL(12, 4)),
//
//			TestSpec
//				.forType(java.time.LocalDateTime.class)
//				.expectDataType(DataTypes.TIMESTAMP(9)),
//
//			TestSpec
//				.forType(java.time.LocalDateTime.class)
//				.configuration(b -> b.defaultSecondPrecision(3))
//				.expectDataType(DataTypes.TIMESTAMP(3)),
//
//			TestSpec
//				.forType(java.time.OffsetDateTime.class)
//				.configuration(b -> b.defaultSecondPrecision(3))
//				.expectDataType(DataTypes.TIMESTAMP_WITH_TIME_ZONE(3)),
//
//			TestSpec
//				.forType(java.time.Instant.class)
//				.configuration(b -> b.defaultSecondPrecision(3))
//				.expectDataType(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)),
//
//			TestSpec
//				.forType(java.time.Duration.class)
//				.configuration(b -> b.defaultSecondPrecision(3))
//				.expectDataType(DataTypes.INTERVAL(DataTypes.SECOND(3))),
//
//			TestSpec
//				.forType(java.time.Period.class)
//				.configuration(b -> b.defaultYearPrecision(2))
//				.expectDataType(DataTypes.INTERVAL(DataTypes.YEAR(2), DataTypes.MONTH())),
//
//			TestSpec
//				.forType(java.time.Period.class)
//				.configuration(b -> b.defaultYearPrecision(0))
//				.expectDataType(DataTypes.INTERVAL(DataTypes.MONTH())),
//
//			TestSpec
//				.forType(Object[][].class)
//				.configuration(b -> b.allowAny(true))
//				.expectDataType(
//					DataTypes.ARRAY(
//						DataTypes.ARRAY(
//							DataTypes.ANY(new GenericTypeInfo<>(Object.class))))),
//
//			TestSpec
//				.forArgumentOf(TableFunction.class, 0, TableFunctionWithMapLevel2.class)
//				.expectDataType(DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BOOLEAN())),
//
//			TestSpec
//				.forArgumentOf(TableFunction.class, 0, TableFunctionWithGenericArray1.class)
//				.expectDataType(DataTypes.ARRAY(DataTypes.INT())),
//
//			TestSpec
//				.forArgumentOf(TableFunction.class, 0, TableFunctionWithHashMap.class)
//				.expectErrorMessage("Unsupported type"),

			TestSpec
				.forType(SimplePojo.class)
				.expectDataType(getSimplePojoDataType(SimplePojo.class))
		);
	}

	@Parameter
	public TestSpec testSpec;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testExtractionFromType() {
		if (testSpec instanceof TypeTestSpec && testSpec.expectedDataType != null) {
			assertThat(
				getConfiguredExtractor().extractDataType(testSpec.type),
				equalTo(testSpec.expectedDataType));
		}
	}

	@Test
	public void testArgumentExtraction() {
		if (testSpec instanceof ArgumentTestSpec && testSpec.expectedDataType != null) {
			final ArgumentTestSpec argumentTestSpec = (ArgumentTestSpec) testSpec;
			assertThat(
				getConfiguredExtractor().extractDataType(
					argumentTestSpec.baseClass,
					argumentTestSpec.argument,
					argumentTestSpec.type),
				equalTo(testSpec.expectedDataType));
		}
	}

	@Test
	public void testExtractionErrorFromType() {
		if (testSpec instanceof TypeTestSpec && testSpec.expectedErrorMessage != null) {
			thrown.expect(ValidationException.class);
			thrown.expectMessage("Could not extract a data type");
			thrown.expectCause(
				anyOf(
					instanceOf(IllegalStateException.class),
					hasProperty("message", contains(testSpec.expectedErrorMessage))));
			getConfiguredExtractor().extractDataType(testSpec.type);
		}
	}

//	@Test
//	public void testArgumentExtractionError() {
//		if (testSpec instanceof ArgumentTestSpec && testSpec.expectedErrorMessage != null) {
//			thrown.expect(ValidationException.class);
//			thrown.expectMessage("Could not extract a data type");
//			thrown.expectCause(
//				anyOf(
//					instanceOf(IllegalStateException.class),
//					hasProperty("message", contains(testSpec.expectedErrorMessage))));
//			getConfiguredExtractor().extractDataType(testSpec.clazz, testSpec.argument);
//		}
//	}

	// --------------------------------------------------------------------------------------------
	// Test utilities
	// --------------------------------------------------------------------------------------------

	private ReflectiveDataTypeConverter getConfiguredExtractor() {
		final ReflectiveDataTypeConverter.Builder builder = ReflectiveDataTypeConverter.newInstance();
		if (testSpec.configuration != null) {
			testSpec.configuration.accept(builder);
		}
		return builder.build();
	}

	private static abstract class TestSpec {

		final Type type;

		@Nullable Consumer<ReflectiveDataTypeConverter.Builder> configuration;

		@Nullable DataType expectedDataType;

		@Nullable String expectedErrorMessage;

		TestSpec(Type type) {
			this.type = type;
		}

		static TestSpec forType(Type type) {
			return new TypeTestSpec(type);
		}

		static TestSpec forArgumentOf(Class<?> baseClass, int argument, Type type) {
			return new ArgumentTestSpec(baseClass, argument, type);
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

	private static class TypeTestSpec extends TestSpec {

		TypeTestSpec(Type type) {
			super(type);
		}
	}

	private static class ArgumentTestSpec extends TestSpec {

		final Class<?> baseClass;
		final int argument;

		ArgumentTestSpec(Class<?> baseClass, int argument, Type type) {
			super(type);
			this.baseClass = baseClass;
			this.argument = argument;
		}
	}

	private static DataType getSimplePojoDataType(Class<?> simplePojoClass) {
		final StructuredType.Builder builder = StructuredType.newInstance(simplePojoClass);
		builder.attributes(
			Arrays.asList(
				new StructuredType.StructuredAttribute("primitiveIntField", new IntType(false)),
				new StructuredType.StructuredAttribute("intField", new IntType(true)),
				new StructuredType.StructuredAttribute("stringField", new VarCharType(VarCharType.MAX_LENGTH)),
				new StructuredType.StructuredAttribute("primitiveBooleanField", new BooleanType())));
		builder.isInstantiable(true);
		builder.isFinal(true);
		final StructuredType structuredType = builder.build();

		final Map<String, DataType> fields = new HashMap<>();
		fields.put("primitiveIntField", DataTypes.INT().notNull().bridgedTo(int.class));
		fields.put("intField", DataTypes.INT());
		fields.put("stringField", DataTypes.STRING());
		fields.put("primitiveBooleanField", DataTypes.BOOLEAN().notNull().bridgedTo(boolean.class));

		return new FieldsDataType(structuredType, simplePojoClass, fields);
	}

	private static DataType getComplexPojoDataType(Class<?> complexPojoClass, Class<?> simplePojoClass) {
		final StructuredType.Builder builder = StructuredType.newInstance(complexPojoClass);
		builder.attributes(
			Arrays.asList(
				new StructuredType.StructuredAttribute("someObject", new TypeInformationAnyType<>(new GenericTypeInfo<>(Object.class))),
				new StructuredType.StructuredAttribute("mapField", new MapType(new VarCharType(VarCharType.MAX_LENGTH), new IntType())),
				new StructuredType.StructuredAttribute("simplePojoField", getSimplePojoDataType(simplePojoClass).logicalType)));
		builder.isInstantiable(true);
		builder.isFinal(true);
		final StructuredType structuredType = builder.build();

		final Map<String, DataType> fields = new HashMap<>();
		fields.put("someObject", DataTypes.ANY(new GenericTypeInfo<>(Object.class)));
		fields.put("mapField", DataTypes.INT());
		fields.put("simplePojoField", getSimplePojoDataType(simplePojoClass));

		return new FieldsDataType(structuredType, simplePojoClass, fields);
	}

	// --------------------------------------------------------------------------------------------
	// Test classes for extraction
	// --------------------------------------------------------------------------------------------

	private static class TableFunctionWithInteger extends TableFunction<Integer> {

	}

	// --------------------------------------------------------------------------------------------

	private static class TableFunctionWithMapLevel0<K, V> extends TableFunction<Map<K, V>> {

	}

	private static class TableFunctionWithMapLevel1<V> extends TableFunctionWithMapLevel0<Long, V> {

	}

	private static class TableFunctionWithMapLevel2 extends TableFunctionWithMapLevel1<Boolean> {

	}

	// --------------------------------------------------------------------------------------------

	private static class TableFunctionWithHashMap extends TableFunction<HashMap<Integer, String>> {

	}

	// --------------------------------------------------------------------------------------------

	private static class TableFunctionWithGenericArray0<T> extends TableFunction<T[]> {

	}

	private static class TableFunctionWithGenericArray1 extends TableFunctionWithGenericArray0<Integer> {

	}

	// --------------------------------------------------------------------------------------------

	public static class ComplexPojo {
		public Object someObject;
		public Map<String, Integer> mapField;
		public SimplePojo simplePojoField;
	}

	public static class SimplePojo {
		public int primitiveIntField;
		public Integer intField;
		public String stringField;
		public boolean primitiveBooleanField;
	}
}
