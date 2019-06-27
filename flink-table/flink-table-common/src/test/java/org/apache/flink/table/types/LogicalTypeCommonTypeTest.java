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
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link LogicalTypeCasts}.
 */
@RunWith(Parameterized.class)
public class LogicalTypeWideningTest {

	@Parameters(name = "{index}: [Types: {0}, To: {1}]")
	public static List<Object[]> testData() {
		return Arrays.asList(
			new Object[][]{
				{DataTypes.SMALLINT(), DataTypes.BIGINT(), true, true},
			}
		);
	}

	@Parameter
	public Set<DataType> types;

	@Parameter(1)
	public DataType result;

	@Test
	public void testWidening() {
		assertThat(
			LogicalTypeCasts.supportsImplicitCast(sourceType.getLogicalType(), targetType.getLogicalType()),
			equalTo(supportsImplicit));
	}

	@Test
	public void testExplicitCasting() {
		assertThat(
			LogicalTypeCasts.supportsExplicitCast(sourceType.getLogicalType(), targetType.getLogicalType()),
			equalTo(supportsExplicit));
	}
}
