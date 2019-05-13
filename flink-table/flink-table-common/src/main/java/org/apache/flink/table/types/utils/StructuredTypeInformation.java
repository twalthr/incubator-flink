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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Generic {@link TypeInformation} that describes structured types such as tuples, POJOs, and Scala
 * case classes.
 *
 * <p>This class is just a placeholder and can be filled with type information provided by bridging
 * APIs. Once the type has been filled the type information behaves as the original type information.
 */
@Internal
public final class StructuredTypeInformation<T> extends CompositeType<T> {

	private final DataType dataType;

	private final TypeInformation<?>[] fieldTypeInfo;

	private @Nullable CompositeType<T> resolvedTypeInfo = null;

	public StructuredTypeInformation(
			Class<T> typeClass,
			DataType fieldsDataType,
			TypeInformation<?>[] fieldTypeInfo) {
		super(typeClass);
		this.dataType = fieldsDataType;
		this.fieldTypeInfo = fieldTypeInfo;
	}

	public void setResolvedTypeInfo(@Nullable CompositeType<T> resolvedTypeInfo) {
		this.resolvedTypeInfo = resolvedTypeInfo;
	}

	public CompositeType<T> getResolvedTypeInfo() {
		if (hasUnresolvedTypeInfo()) {
			throw new TableException("Type information for structured type has not been resolved.");
		}
		return resolvedTypeInfo;
	}

	public boolean hasUnresolvedTypeInfo() {
		return resolvedTypeInfo == null;
	}

	public DataType getDataType() {
		return dataType;
	}

	public TypeInformation<?>[] getFieldTypeInfo() {
		return fieldTypeInfo;
	}

	@Override
	public void getFlatFields(String fieldExpression, int offset, List<FlatFieldDescriptor> result) {
		getResolvedTypeInfo().getFlatFields(fieldExpression, offset, result);
	}

	@Override
	public <X> TypeInformation<X> getTypeAt(String fieldExpression) {
		return getResolvedTypeInfo().getTypeAt(fieldExpression);
	}

	@Override
	public <X> TypeInformation<X> getTypeAt(int pos) {
		return getResolvedTypeInfo().getTypeAt(pos);
	}

	@Override
	protected TypeComparatorBuilder<T> createTypeComparatorBuilder() {
		// we call createComparator() directly
		throw new TableException("Unsupported operation. This should not be called.");
	}

	@Override
	public TypeComparator<T> createComparator(
			int[] logicalKeyFields,
			boolean[] orders,
			int logicalFieldOffset,
			ExecutionConfig config) {
		return getResolvedTypeInfo().createComparator(
			logicalKeyFields,
			orders,
			logicalFieldOffset,
			config);
	}

	@Override
	public String[] getFieldNames() {
		return getResolvedTypeInfo().getFieldNames();
	}

	@Override
	public int getFieldIndex(String fieldName) {
		return getResolvedTypeInfo().getFieldIndex(fieldName);
	}

	@Override
	public boolean isBasicType() {
		return getResolvedTypeInfo().isBasicType();
	}

	@Override
	public boolean isTupleType() {
		return getResolvedTypeInfo().isTupleType();
	}

	@Override
	public int getArity() {
		return getResolvedTypeInfo().getArity();
	}

	@Override
	public int getTotalFields() {
		return getResolvedTypeInfo().getArity();
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		return getResolvedTypeInfo().createSerializer(config);
	}
}
