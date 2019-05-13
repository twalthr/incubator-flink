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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.UserDefinedType;
import org.apache.flink.table.types.utils.StructuredTypeInformation;
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter;
import org.apache.flink.types.Row;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link TypeInfoDataTypeConverter}.
 */
@RunWith(Parameterized.class)
public class TypeInfoDataTypeConverterTest {

	@Parameters(name = "{index}: {0}={1}")
	public static List<Object[]> typeInfo() {
		return Arrays.asList(
			new Object[][]{
				{longTypeInfo(), longType(), longTypeInfo()},

				{booleanTypeInfo(), booleanType(), booleanTypeInfo()},

				{userTypeInfo(), userType(), userUnresolvedTypeInfo()},

				{rowTypeInfo(), rowType(), rowTypeInfo()},

				{tupleTypeInfo(), tupleType(), tupleUnresolvedTypeInfo()}
			}
		);
	}

	@Parameter
	public TypeInformation<?> inputTypeInfo;

	@Parameter(1)
	public DataType dataType;

	@Parameter(2)
	public TypeInformation<?> outputTypeInfo;

	@Test
	public void testTypeInfoToDataTypeConversion() {
		final DataType d = TypeInfoDataTypeConverter.toDataType(inputTypeInfo);
		assertEquals(dataType, d);
	}

	@Test
	public void testDataTypeToTypeInfoConversion() {
		final TypeInformation<?> ti = TypeInfoDataTypeConverter.toTypeInfo(dataType);
		assertEquals(outputTypeInfo, ti);
	}

	// --------------------------------------------------------------------------------------------

	private static TypeInformation<Long> longTypeInfo() {
		return Types.LONG;
	}

	private static DataType longType() {
		return DataTypes.BIGINT()
			.notNull()
			.bridgedTo(Long.class);
	}

	private static TypeInformation<Boolean> booleanTypeInfo() {
		return Types.BOOLEAN;
	}

	private static DataType booleanType() {
		return DataTypes.BOOLEAN()
			.notNull()
			.bridgedTo(Boolean.class);
	}

	/**
	 * Test POJO.
	 */
	public static class User {

		public Integer favoriteNumber;

		public int age;

		public String name;

		public LocalDateTime lastUpdated;
	}

	private static TypeInformation<User> userTypeInfo() {
		return Types.POJO(User.class);
	}

	private static DataType userType() {
		final LogicalType structuredType = new StructuredType.Builder(
				new UserDefinedType.TypeIdentifier(User.class.getName()),
				Arrays.asList(
					new StructuredType.StructuredAttribute(
						"age",
						ageType().getLogicalType()),
					new StructuredType.StructuredAttribute(
						"favoriteNumber",
						favoriteNumberType().getLogicalType()),
					new StructuredType.StructuredAttribute(
						"lastUpdated",
						lastUpdatedType().getLogicalType()),
					new StructuredType.StructuredAttribute(
						"name",
						nameType().getLogicalType())))
			.setImplementationClass(User.class)
			.setInstantiable(false)
			.setFinal(true)
			.setComparision(StructuredType.StructuredComparision.NONE)
			.setNullable(true)
			.build();

		final Map<String, DataType> fields = new HashMap<>();
		fields.put("favoriteNumber", favoriteNumberType());
		fields.put("age", ageType());
		fields.put("name", nameType());
		fields.put("lastUpdated", lastUpdatedType());

		return new FieldsDataType(structuredType, fields)
			.nullable()
			.bridgedTo(User.class);
	}

	private static DataType ageType() {
		return DataTypes.INT()
			.notNull()
			.bridgedTo(int.class);
	}

	private static DataType favoriteNumberType() {
		return DataTypes.INT()
			.nullable()
			.bridgedTo(Integer.class);
	}

	private static DataType nameType() {
		return DataTypes.STRING()
			.nullable()
			.bridgedTo(String.class);
	}

	private static DataType lastUpdatedType() {
		return DataTypes.TIMESTAMP(9)
			.nullable()
			.bridgedTo(LocalDateTime.class);
	}

	private static TypeInformation<Row> rowTypeInfo() {
		return Types.ROW_NAMED(
			new String[]{"nested0", "nested1", "nested2", "nested3"},
			Types.LONG,
			Types.MAP(Types.BYTE, Types.DOUBLE),
			Types.INSTANT,
			Types.GENERIC(TypeInfoDataTypeConverterTest.class)
		);
	}

	private static DataType rowType() {
		return DataTypes.ROW(
				FIELD("nested0", nested0()),
				FIELD("nested1", nested1()),
				FIELD("nested2", nested2()),
				FIELD("nested3", nested3()))
			.notNull()
			.bridgedTo(Row.class);
	}

	private static DataType nested0() {
		return DataTypes.BIGINT()
			.nullable()
			.bridgedTo(Long.class);
	}

	private static DataType nested1() {
		return DataTypes.MAP(
				DataTypes.TINYINT().notNull().bridgedTo(Byte.class),
				DataTypes.DOUBLE().nullable().bridgedTo(Double.class))
			.nullable()
			.bridgedTo(Map.class);
	}

	private static DataType nested2() {
		return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)
			.nullable()
			.bridgedTo(Instant.class);
	}

	private static DataType nested3() {
		return DataTypes.ANY(Types.GENERIC(TypeInfoDataTypeConverterTest.class))
			.nullable()
			.bridgedTo(TypeInfoDataTypeConverterTest.class);
	}

	private static <T extends Tuple> TypeInformation<T> tupleTypeInfo() {
		return Types.TUPLE(
			longTypeInfo(),
			booleanTypeInfo(),
			userTypeInfo(),
			rowTypeInfo()
		);
	}

	private static DataType tupleType() {
		final LogicalType structuredType = new StructuredType.Builder(
				new UserDefinedType.TypeIdentifier(Tuple4.class.getName()),
				Arrays.asList(
					new StructuredType.StructuredAttribute("f0", longType().getLogicalType()),
					new StructuredType.StructuredAttribute("f1", booleanType().getLogicalType()),
					new StructuredType.StructuredAttribute("f2", userType().getLogicalType()),
					new StructuredType.StructuredAttribute("f3", rowType().getLogicalType())))
			.setImplementationClass(Tuple4.class)
			.setInstantiable(false)
			.setFinal(true)
			.setComparision(StructuredType.StructuredComparision.EQUALS)
			.setNullable(false)
			.build();

		final Map<String, DataType> fields = new HashMap<>();
		fields.put("f0", longType());
		fields.put("f1", booleanType());
		fields.put("f2", userType());
		fields.put("f3", rowType());

		return new FieldsDataType(structuredType, fields)
			.notNull()
			.bridgedTo(Tuple4.class);
	}

	private static TypeInformation<User> userUnresolvedTypeInfo() {
		final CompositeType<User> userType = ((CompositeType<User>) userTypeInfo());
		final TypeInformation<?>[] fieldTypes = IntStream.range(0, userType.getArity())
			.mapToObj(userType::getTypeAt)
			.toArray(TypeInformation[]::new);
		return new StructuredTypeInformation<>(
			User.class,
			userType(),
			fieldTypes);
	}

	private static TypeInformation<Tuple4> tupleUnresolvedTypeInfo() {
		return new StructuredTypeInformation<>(
			Tuple4.class,
			tupleType(),
			new TypeInformation<?>[]{
				longTypeInfo(),
				booleanTypeInfo(),
				userUnresolvedTypeInfo(),
				rowTypeInfo()
			});
	}
}
