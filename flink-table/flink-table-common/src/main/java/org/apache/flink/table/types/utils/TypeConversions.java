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

package org.apache.flink.table.types.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.stream.Stream;

/**
 * Utilities for interoperability of {@link TypeInformation}, {@link DataType}, and
 * {@link LogicalType}.
 */
public final class TypeConversions {

	public static DataType fromLegacyInfoToDataType(TypeInformation<?> typeInfo) {
		return LegacyTypeInfoDataTypeConverter.toDataType(typeInfo);
	}

	public static DataType[] fromLegacyInfoToDataType(TypeInformation<?>[] typeInfo) {
		return Stream.of(typeInfo)
			.map(TypeConversions::fromLegacyInfoToDataType)
			.toArray(DataType[]::new);
	}

	public static TypeInformation<?> fromDataTypeToLegacyInfo(DataType dataType) {
		return LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(dataType);
	}

	public static TypeInformation<?>[] fromDataTypeToLegacyInfo(DataType[] dataType) {
		return Stream.of(dataType)
			.map(TypeConversions::fromDataTypeToLegacyInfo)
			.toArray(TypeInformation[]::new);
	}









//	public static DataType fromLogicalToDataType(LogicalType logicalType) {
//		return LogicalTypeDataTypeConverter.toDataType(logicalType);
//	}
//
//	public static DataType[] fromLogicalToDataType(LogicalType[] logicalTypes) {
//		return Stream.of(logicalTypes)
//			.map(LogicalTypeDataTypeConverter::toDataType)
//			.toArray(DataType[]::new);
//	}
//
//	public static LogicalType fromDataToLogicalType(DataType dataType) {
//		return dataType.getLogicalType();
//	}
//
//	public static LogicalType[] fromDataToLogicalType(DataType[] dataTypes) {
//		return Stream.of(dataTypes)
//			.map(TypeConversions::fromDataToLogicalType)
//			.toArray(LogicalType[]::new);
//	}
//
//	public static DataType fromInfoToDataType(TypeInformation<?> typeInfo, boolean withBridging) {
//		return TypeInfoDataTypeConverter.toDataType(typeInfo, withBridging);
//	}
//
//	public static DataType fromInfoToDataType(TypeInformation<?> typeInfo) {
//		return fromInfoToDataType(typeInfo, true);
//	}
//
//	public static DataType[] fromInfoToDataType(TypeInformation<?>[] typeInfo, boolean withBridging) {
//		return Stream.of(typeInfo)
//			.map(ti -> fromInfoToDataType(ti, withBridging))
//			.toArray(DataType[]::new);
//	}
//
//	public static DataType[] fromInfoToDataType(TypeInformation<?>[] typeInfo) {
//		return fromInfoToDataType(typeInfo, true);
//	}
//
//	public static TypeInformation<?> fromDataTypeToInfo(DataType dataType) {
//		return TypeInfoDataTypeConverter.toTypeInfo(dataType);
//	}
//
//	public static TypeInformation<?>[] fromDataTypeToInfo(DataType[] dataTypes) {
//		return Stream.of(dataTypes)
//			.map(TypeConversions::fromDataTypeToInfo)
//			.toArray(TypeInformation[]::new);
//	}
//
//	public static LogicalType fromInfoToLogicalType(TypeInformation<?> typeInfo) {
//		return fromDataToLogicalType(fromInfoToDataType(typeInfo, false));
//	}
//
//	public static LogicalType[] fromInfoToLogicalType(TypeInformation<?>[] typeInfo) {
//		return fromDataToLogicalType(fromInfoToDataType(typeInfo, false));
//	}
//
//	public static TypeInformation<?> fromLogicalTypeToInfo(LogicalType logicalType) {
//		return TypeInfoDataTypeConverter.toTypeInfo(fromLogicalToDataType(logicalType));
//	}
//
//	public static TypeInformation<?>[] fromLogicalTypeToInfo(LogicalType[] logicalType) {
//		return fromDataTypeToInfo(fromLogicalToDataType(logicalType));
//	}

	private TypeConversions() {
		// no instance
	}
}
