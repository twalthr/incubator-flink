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

package org.apache.flink.table.types.logical.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import java.util.stream.Collectors;

/**
 * Utilities for handling {@link LogicalType}s.
 */
@Internal
public final class LogicalTypeUtils {

	private static final TimeAttributeRemover TIME_ATTRIBUTE_REMOVER = new TimeAttributeRemover();

	public static LogicalType removeTimeAttributes(LogicalType logicalType) {
		return logicalType.accept(TIME_ATTRIBUTE_REMOVER);
	}

	/**
	 * Returns the conversion class for the given {@link LogicalType} that is used by the
	 * table runtime as internal data structure.
	 *
	 * @see RowData
	 */
	public static Class<?> toInternalConversionClass(LogicalType type) {
		// ordered by type root definition
		switch (type.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				return StringData.class;
			case BOOLEAN:
				return Boolean.class;
			case BINARY:
			case VARBINARY:
				return byte[].class;
			case DECIMAL:
				return DecimalData.class;
			case TINYINT:
				return Byte.class;
			case SMALLINT:
				return Short.class;
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
				return Integer.class;
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return Long.class;
			case FLOAT:
				return Float.class;
			case DOUBLE:
				return Double.class;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return TimestampData.class;
			case TIMESTAMP_WITH_TIME_ZONE:
				throw new UnsupportedOperationException();
			case ARRAY:
				return ArrayData.class;
			case MULTISET:
			case MAP:
				return MapData.class;
			case ROW:
			case STRUCTURED_TYPE:
				return RowData.class;
			case DISTINCT_TYPE:
				return toInternalConversionClass(((DistinctType) type).getSourceType());
			case RAW:
				return RawValueData.class;
			case NULL:
			case SYMBOL:
			case UNRESOLVED:
			default:
				throw new IllegalArgumentException();
		}
	}

	/**
	 * Converts the given composite type to a {@link RowType}.
	 *
	 * @see LogicalTypeChecks#isCompositeType(LogicalType)
	 */
	public static RowType toRowType(LogicalType type) {
		switch (type.getTypeRoot()) {
			case ROW:
				return (RowType) type;
			case STRUCTURED_TYPE:
				final StructuredType structuredType = (StructuredType) type;
				return new RowType(
					structuredType.isNullable(),
					structuredType.getAttributes().stream()
						.map(a -> new RowField(a.getName(), a.getType(), a.getDescription().orElse(null)))
						.collect(Collectors.toList()));
			case DISTINCT_TYPE:
				return toRowType(((DistinctType) type).getSourceType());
			default:
				throw new IllegalArgumentException();
		}
	}

	// --------------------------------------------------------------------------------------------

	private static class TimeAttributeRemover extends LogicalTypeDuplicator {

		@Override
		public LogicalType visit(TimestampType timestampType) {
			return new TimestampType(
				timestampType.isNullable(),
				timestampType.getPrecision());
		}

		@Override
		public LogicalType visit(ZonedTimestampType zonedTimestampType) {
			return new ZonedTimestampType(
				zonedTimestampType.isNullable(),
				zonedTimestampType.getPrecision());
		}

		@Override
		public LogicalType visit(LocalZonedTimestampType localZonedTimestampType) {
			return new LocalZonedTimestampType(
				localZonedTimestampType.isNullable(),
				localZonedTimestampType.getPrecision());
		}
	}

	private LogicalTypeUtils() {
		// no instantiation
	}
}
