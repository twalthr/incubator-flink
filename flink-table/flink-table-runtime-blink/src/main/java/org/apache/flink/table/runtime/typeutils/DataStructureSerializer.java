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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.types.InternalSerializers;
import org.apache.flink.table.types.DataType;

import java.io.IOException;
import java.util.Objects;

public class DataStructureSerializer<I, E> extends TypeSerializer<E> {

	private final DataType dataType;

	private transient TypeSerializer<I> serializer;
	private transient DataStructureConverter<I, E> converter;

	private DataStructureSerializer(DataType dataType) {
		this.dataType = dataType;
	}

	public static <I, E>  DataStructureSerializer<I, E> of(DataType dataType) {
		final DataStructureSerializer<I, E> serializer = new DataStructureSerializer<>(dataType);
		serializer.initializeIfNeeded(); // fail early if data type is not supported
		return serializer;
	}

	@SuppressWarnings("unchecked")
	private void initializeIfNeeded() {
		if (serializer == null) {
			converter = (DataStructureConverter<I, E>) DataStructureConverters.getConverter(dataType);
			serializer = (TypeSerializer<I>) InternalSerializers.create(dataType.getLogicalType());
		}
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<E> duplicate() {
		return new DataStructureSerializer<>(dataType);
	}

	@Override
	public E createInstance() {
		initializeIfNeeded();
		final I instance = serializer.createInstance();
		return converter.toExternalOrNull(instance);
	}

	@Override
	public E copy(E from) {
		initializeIfNeeded();
		final I internalFrom = converter.toInternalOrNull(from);
		final I copy = serializer.copy(internalFrom);
		return converter.toExternalOrNull(copy);
	}

	@Override
	public E copy(E from, E reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		initializeIfNeeded();
		return serializer.getLength();
	}

	@Override
	public void serialize(E record, DataOutputView target) throws IOException {
		initializeIfNeeded();
		final I internalRecord = converter.toInternalOrNull(record);
		serializer.serialize(internalRecord, target);
	}

	@Override
	public E deserialize(DataInputView source) throws IOException {
		initializeIfNeeded();
		final I internalRecord = serializer.deserialize(source);
		return converter.toExternalOrNull(internalRecord);
	}

	@Override
	public E deserialize(E reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		initializeIfNeeded();
		serializer.copy(source, target);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		DataStructureSerializer<?, ?> that = (DataStructureSerializer<?, ?>) o;
		return dataType.equals(that.dataType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(dataType);
	}

	@Override
	public TypeSerializerSnapshot<E> snapshotConfiguration() {
		throw new UnsupportedOperationException();
	}
}
