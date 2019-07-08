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

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link LogicalTypeParser}.
 */
@RunWith(Parameterized.class)
public class LogicalTypeParserTest {

	@Parameters(name = "{index}: [From: {0}, To: {1}]")
	public static List<Object[]> testData() {
		return Arrays.asList(
			new Object[][]{

				{"CHAR", new CharType()},

				{"CHAR NOT NULL", new CharType().copy(false)},

				{"CHAR NULL", new CharType()},

				{"CHAR(33)", new CharType(33)},

				{"VARCHAR", new VarCharType()},

				{"VARCHAR(33)", new VarCharType(33)},

				{"STRING", new VarCharType(VarCharType.MAX_LENGTH)},

				{"BOOLEAN", new BooleanType()},

				{"BINARY", new BinaryType()},

				{"BINARY(33)", new BinaryType(33)},

				{"VARBINARY", new VarBinaryType()},

				{"VARBINARY(33)", new VarBinaryType(33)},

				{"BYTES", new VarBinaryType(VarBinaryType.MAX_LENGTH)},

				{"DECIMAL", new DecimalType()},

				{"DEC", new DecimalType()},

				{"NUMERIC", new DecimalType()},

				{"DECIMAL(10)", new DecimalType(10)},

				{"DEC(10)", new DecimalType(10)},

				{"NUMERIC(10)", new DecimalType(10)},

				{"DECIMAL(10, 3)", new DecimalType(10, 3)},

				{"DEC(10, 3)", new DecimalType(10, 3)},

				{"NUMERIC(10, 3)", new DecimalType(10, 3)},

				{"TINYINT", new TinyIntType()},

				{"SMALLINT", new SmallIntType()},

				{"INTEGER", new IntType()},

				{"INT", new IntType()},

				{"BIGINT", new BigIntType()},

				{"FLOAT", new FloatType()},

				{"DOUBLE", new DoubleType()},

				{"DOUBLE PRECISION", new DoubleType()},

				{"DATE", new DateType()},

				{"TIME", new TimeType()},

				{"TIME(3)", new TimeType(3)},

				{"TIME WITHOUT TIME ZONE", new TimeType()},

				{"TIME(3) WITHOUT TIME ZONE", new TimeType(3)},

				{"TIMESTAMP", new TimestampType()},

				{"TIMESTAMP(3)", new TimestampType(3)},

				{"TIMESTAMP WITHOUT TIME ZONE", new TimestampType()},

				{"TIMESTAMP(3) WITHOUT TIME ZONE", new TimestampType(3)},

				{"TIMESTAMP WITH TIME ZONE", new ZonedTimestampType()},

				{"TIMESTAMP(3) WITH TIME ZONE", new ZonedTimestampType(3)},

				{"TIMESTAMP WITH LOCAL TIME ZONE", new LocalZonedTimestampType()},

				{"TIMESTAMP(3) WITH LOCAL TIME ZONE", new LocalZonedTimestampType(3)},

				{
					"INTERVAL YEAR",
					new YearMonthIntervalType(YearMonthResolution.YEAR)
				},

				{
					"INTERVAL YEAR(4)",
					new YearMonthIntervalType(YearMonthResolution.YEAR, 4)
				},

				{
					"INTERVAL MONTH",
					new YearMonthIntervalType(YearMonthResolution.MONTH)
				},

				{
					"INTERVAL YEAR TO MONTH",
					new YearMonthIntervalType(YearMonthResolution.YEAR_TO_MONTH)
				},

				{
					"INTERVAL YEAR(4) TO MONTH",
					new YearMonthIntervalType(YearMonthResolution.YEAR_TO_MONTH, 4)
				},
			}
		);
	}

	@Parameter
	public String typeString;

	@Parameter(1)
	public LogicalType type;

	@Test
	public void testParsing() {
		assertThat(
			LogicalTypeParser.parse(typeString),
			equalTo(type));
	}

	@Test
	public void testSerializableParsing() {
		assertThat(
			LogicalTypeParser.parse(type.asSerializableString()),
			equalTo(type));
	}
}
