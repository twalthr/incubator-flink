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

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link LogicalTypeCasts}.
 */
@RunWith(Parameterized.class)
public class LogicalTypeCommonTypeTest {

	@Parameters(name = "{index}: [Types: {0}, To: {1}]")
	public static List<Object[]> testData() {
		return Arrays.asList(
			new Object[][]{

				// simple types
				{
					Arrays.asList(new IntType(), new IntType()),
					new IntType()
				},

				// incompatible types
				{
					Arrays.asList(new IntType(), new ArrayType(new IntType())),
					null
				},

				// incompatible types
				{
					Arrays.asList(new IntType(), new VarCharType(23)),
					null
				},

				// NOT NULL types
				{
					Arrays.asList(new IntType(false), new IntType(false)),
					new IntType(false)
				},

				// NOT NULL with different types
				{
					Arrays.asList(new IntType(true), new BigIntType(false)),
					new BigIntType()
				},

				// NULL only
				{
					Arrays.asList(new NullType(), new NullType()),
					new NullType()
				},

				// NULL with other types
				{
					Arrays.asList(new NullType(), new IntType(), new IntType()),
					new IntType()
				},

				// ARRAY types with same element type
				{
					Arrays.asList(new ArrayType(new IntType()), new ArrayType(new IntType())),
					new ArrayType(new IntType())
				},

				// ARRAY types with different element types
				{
					Arrays.asList(new ArrayType(new BigIntType()), new ArrayType(new IntType())),
					new ArrayType(new BigIntType())
				},

				// MULTISET types with different element type
				{
					Arrays.asList(new MultisetType(new BigIntType()), new MultisetType(new IntType())),
					new MultisetType(new BigIntType())
				},

				// ROW type with different element types
				{
					Arrays.asList(
						RowType.of(new IntType(), new IntType(), new BigIntType()),
						RowType.of(new BigIntType(), new IntType(), new IntType())),
					RowType.of(new BigIntType(), new IntType(), new BigIntType())
				},

				// CHAR types of different length
				{
					Arrays.asList(new CharType(2), new CharType(4)),
					new CharType(4)
				},

				// VARCHAR types of different length
				{
					Arrays.asList(new VarCharType(2), new VarCharType(VarCharType.MAX_LENGTH)),
					new VarCharType(VarCharType.MAX_LENGTH)
				},

				// mixed VARCHAR and CHAR types
				{
					Arrays.asList(new VarCharType(2), new CharType(5)),
					new VarCharType(5)
				},

				// more mixed VARCHAR and CHAR types
				{
					Arrays.asList(new CharType(5), new VarCharType(2), new VarCharType(7)),
					new VarCharType(7)
				},

				// mixed BINARY and VARBINARY types
				{
					Arrays.asList(new BinaryType(5), new VarBinaryType(2), new VarBinaryType(7)),
					new VarBinaryType(7)
				},

				// two APPROXIMATE_NUMERIC types
				{
					Arrays.asList(new DoubleType(), new FloatType()),
					new DoubleType()
				},

				// one APPROXIMATE_NUMERIC and one DECIMAL type
				{
					Arrays.asList(new DoubleType(), new DecimalType(2, 2)),
					new DoubleType()
				},

				// one APPROXIMATE_NUMERIC and one EXACT_NUMERIC type
				{
					Arrays.asList(new DoubleType(), new IntType()),
					new DoubleType()
				},

				// two APPROXIMATE_NUMERIC and one DECIMAL type
				{
					Arrays.asList(new DecimalType(2, 2), new DoubleType(), new FloatType()),
					new DoubleType()
				},

				// DECIMAL precision and scale merging
				{
					Arrays.asList(new DecimalType(2, 2), new DecimalType(5, 2), new DecimalType(7, 5)),
					new DecimalType(8, 5)
				},

				// DECIMAL precision and scale merging with other EXACT_NUMERIC types
				{
					Arrays.asList(new DecimalType(2, 2), new IntType(), new BigIntType()),
					new DecimalType(21, 2)
				},

				// unsupported time merging
				{
					Arrays.asList(new DateType(), new DateType(), new TimeType()),
					null
				},

				// time precision merging
				{
					Arrays.asList(new TimeType(3), new TimeType(2)),
					new TimeType(3)
				}

			}
		);
	}

	@Parameter
	public List<LogicalType> types;

	@Parameter(1)
	public LogicalType commonType;

	@Test
	public void testCommonType() {
		assertThat(
			LogicalTypeCasts.findCommonType(types),
			equalTo(Optional.ofNullable(commonType)));
	}
}
