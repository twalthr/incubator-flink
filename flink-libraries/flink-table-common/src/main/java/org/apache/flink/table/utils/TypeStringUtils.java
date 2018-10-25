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

package org.apache.flink.table.utils;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.MultisetTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.TableException;

public class TypeStringUtils {

	private static final String VARCHAR = "VARCHAR";
	private static final String STRING = "STRING";
	private static final String BOOLEAN = "BOOLEAN";
	private static final String BYTE = "BYTE";
	private static final String TINYINT = "TINYINT";
	private static final String SHORT = "SHORT";
	private static final String SMALLINT = "SMALLINT";
	private static final String INT = "INT";
	private static final String LONG = "LONG";
	private static final String BIGINT = "BIGINT";
	private static final String FLOAT = "FLOAT";
	private static final String DOUBLE = "DOUBLE";
	private static final String DECIMAL = "DECIMAL";
	private static final String SQL_DATE = "SQL_DATE";
	private static final String DATE = "DATE";
	private static final String SQL_TIME = "SQL_TIME";
	private static final String TIME = "TIME";
	private static final String SQL_TIMESTAMP = "SQL_TIMESTAMP";
	private static final String TIMESTAMP = "TIMESTAMP";
	private static final String ROW = "ROW";
	private static final String ANY = "ANY";
	private static final String POJO = "POJO";
	private static final String MAP = "MAP";
	private static final String MULTISET = "MULTISET";
	private static final String PRIMITIVE_ARRAY = "PRIMITIVE_ARRAY";
	private static final String OBJECT_ARRAY = "OBJECT_ARRAY";

	public static TypeInformation<?> readTypeInfo(String typeString) {
		return null;
	}

	public static String writeTypeInfo(TypeInformation<?> typeInfo) {
		if (typeInfo.equals(Types.STRING)) {
			return VARCHAR;
		} else if (typeInfo.equals(Types.BOOLEAN)) {
			return BOOLEAN;
		} else if (typeInfo.equals(Types.BYTE)) {
			return TINYINT;
		} else if (typeInfo.equals(Types.SHORT)) {
			return SMALLINT;
		} else if (typeInfo.equals(Types.INT)) {
			return INT;
		} else if (typeInfo.equals(Types.LONG)) {
			return BIGINT;
		} else if (typeInfo.equals(Types.FLOAT)) {
			return FLOAT;
		} else if (typeInfo.equals(Types.DOUBLE)) {
			return DOUBLE;
		} else if (typeInfo.equals(Types.BIG_DEC)) {
			return DECIMAL;
		} else if (typeInfo.equals(Types.SQL_DATE)) {
			return DATE;
		} else if (typeInfo.equals(Types.SQL_TIME)) {
			return TIME;
		} else if (typeInfo.equals(Types.SQL_TIMESTAMP)) {
			return TIMESTAMP;
		} else if (typeInfo instanceof RowTypeInfo) {
			final RowTypeInfo rt = (RowTypeInfo) typeInfo;
			final String[] fieldNames = rt.getFieldNames();
			final TypeInformation<?>[] fieldTypes = rt.getFieldTypes();

			final StringBuilder result = new StringBuilder();
			result.append(ROW);
			result.append('<');
			for (int i = 0; i < fieldNames.length; i++) {
				// escape field name if it contains spaces
				if (!fieldNames[i].matches("\\S+")) {
					result.append('\"');
					result.append(EncodingUtils.escapeJava(fieldNames[i]));
					result.append('\"');
				} else {
					result.append(fieldNames[i]);
				}
				result.append(' ');
				result.append(writeTypeInfo(fieldTypes[i]));
				if (i < fieldNames.length - 1) {
					result.append(", ");
				}
			}
			result.append('>');
			return result.toString();
		} else if (typeInfo instanceof GenericTypeInfo) {
			return ANY + '<' + typeInfo.getTypeClass().getName() + '>';
		} else if (typeInfo instanceof PojoTypeInfo) {
			// we only support very simple POJOs that only contain extracted fields
			// (not manually specified)
			TypeInformation<?> extractedPojo;
			try {
				extractedPojo = TypeExtractor.createTypeInfo(typeInfo.getTypeClass());
			} catch (InvalidTypesException e) {
				extractedPojo = null;
			}
			if (extractedPojo == null || !typeInfo.equals(extractedPojo)) {
				throw new TableException(
					"A string representation for custom POJO types is not supported yet.");
			}
			return POJO + '<' + typeInfo.getTypeClass().getName() + '>';
		} else if (typeInfo instanceof PrimitiveArrayTypeInfo) {
			final PrimitiveArrayTypeInfo arrayTypeInfo = (PrimitiveArrayTypeInfo) typeInfo;
			return PRIMITIVE_ARRAY + '<' + writeTypeInfo(arrayTypeInfo.getComponentType()) + '>';
		} else if (typeInfo instanceof ObjectArrayTypeInfo) {
			final ObjectArrayTypeInfo arrayTypeInfo = (ObjectArrayTypeInfo) typeInfo;
			return OBJECT_ARRAY + '<' + writeTypeInfo(arrayTypeInfo.getComponentInfo()) + '>';
		} else if (typeInfo instanceof MultisetTypeInfo) {
			final MultisetTypeInfo multisetTypeInfo = (MultisetTypeInfo) typeInfo;
			return MULTISET + '<' + writeTypeInfo(multisetTypeInfo.getElementTypeInfo()) + '>';
		} else if (typeInfo instanceof MapTypeInfo) {
			final MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
			final String keyTypeInfo = writeTypeInfo(mapTypeInfo.getKeyTypeInfo());
			final String valueTypeInfo = writeTypeInfo(mapTypeInfo.getValueTypeInfo());
			return MAP + '<' + keyTypeInfo + ", " + valueTypeInfo + '>';
		} else {
			return ANY + '<' + typeInfo.getTypeClass().getName() + ", " + serialize(typeInfo) + '>';
		}
	}
}
