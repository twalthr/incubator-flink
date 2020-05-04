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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.api.java.typeutils.runtime.RowMaskUtils.ROW_KIND_OFFSET;
import static org.apache.flink.api.java.typeutils.runtime.RowMaskUtils.fillMask;
import static org.apache.flink.api.java.typeutils.runtime.RowMaskUtils.readIntoAndCopyMask;
import static org.apache.flink.api.java.typeutils.runtime.RowMaskUtils.readIntoMask;
import static org.apache.flink.api.java.typeutils.runtime.RowMaskUtils.readKindFromMask;
import static org.apache.flink.api.java.typeutils.runtime.RowMaskUtils.writeMask;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Serializer for {@link Row}.
 *
 * <p>It uses the following serialization format:
 * <pre>
 *     |bitmask|field|field|....
 * </pre>
 * The bitmask consist of {@link RowMaskUtils#ROW_KIND_OFFSET} bits for encoding the {@link RowKind}
 * and n bits for whether a field is null.
 *<p>
 */
@Internal
public final class RowSerializer extends TypeSerializer<Row> {

	private static final long serialVersionUID = 2L;

	private final TypeSerializer<Object>[] fieldSerializers;

	private final int arity;

	private final boolean[] mask;

	private final boolean legacyModeEnabled;

	public RowSerializer(TypeSerializer<?>[] fieldSerializers) {
		this(fieldSerializers, false);
	}

	@SuppressWarnings("unchecked")
	private RowSerializer(TypeSerializer<?>[] fieldSerializers, boolean legacyModeEnabled) {
		this.fieldSerializers = (TypeSerializer<Object>[]) checkNotNull(fieldSerializers);
		this.arity = fieldSerializers.length;
		this.mask = new boolean[(legacyModeEnabled ? 0 : ROW_KIND_OFFSET) + fieldSerializers.length];
		this.legacyModeEnabled = legacyModeEnabled;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<Row> duplicate() {
		TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer[fieldSerializers.length];
		for (int i = 0; i < fieldSerializers.length; i++) {
			duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
		}
		return new RowSerializer(duplicateFieldSerializers);
	}

	@Override
	public Row createInstance() {
		return new Row(fieldSerializers.length);
	}

	@Override
	public Row copy(Row from) {
		int len = fieldSerializers.length;

		if (from.getArity() != len) {
			throw new RuntimeException("Row arity of from does not match serializers.");
		}

		Row result = new Row(from.getKind(), len);
		for (int i = 0; i < len; i++) {
			Object fromField = from.getField(i);
			if (fromField != null) {
				Object copy = fieldSerializers[i].copy(fromField);
				result.setField(i, copy);
			}
			else {
				result.setField(i, null);
			}
		}
		return result;
	}

	@Override
	public Row copy(Row from, Row reuse) {
		int len = fieldSerializers.length;

		// cannot reuse, do a non-reuse copy
		if (reuse == null) {
			return copy(from);
		}

		if (from.getArity() != len || reuse.getArity() != len) {
			throw new RuntimeException(
				"Row arity of reuse or from is incompatible with this RowSerializer.");
		}

		reuse.setKind(from.getKind());

		for (int i = 0; i < len; i++) {
			Object fromField = from.getField(i);
			if (fromField != null) {
				Object reuseField = reuse.getField(i);
				if (reuseField != null) {
					Object copy = fieldSerializers[i].copy(fromField, reuseField);
					reuse.setField(i, copy);
				}
				else {
					Object copy = fieldSerializers[i].copy(fromField);
					reuse.setField(i, copy);
				}
			}
			else {
				reuse.setField(i, null);
			}
		}
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	public int getArity() {
		return arity;
	}

	@Override
	public void serialize(Row record, DataOutputView target) throws IOException {
		final int len = fieldSerializers.length;

		if (record.getArity() != len) {
			throw new RuntimeException("Row arity of from does not match serializers.");
		}

		// write bitmask
		fillMask(len, record, mask);
		writeMask(mask, target);

		// serialize non-null fields
		for (int fieldPos = 0; fieldPos < len; fieldPos++) {
			if (!mask[ROW_KIND_OFFSET + fieldPos]) {
				fieldSerializers[fieldPos].serialize(record.getField(fieldPos), target);
			}
		}
	}

	@Override
	public Row deserialize(DataInputView source) throws IOException {
		final int len = fieldSerializers.length;

		// read bitmask
		readIntoMask(source, mask);
		final Row result = new Row(legacyModeEnabled ? RowKind.INSERT : readKindFromMask(mask), len);

		// deserialize fields
		for (int fieldPos = 0; fieldPos < len; fieldPos++) {
			if (!mask[(legacyModeEnabled ? 0 : ROW_KIND_OFFSET) + fieldPos]) {
				result.setField(fieldPos, fieldSerializers[fieldPos].deserialize(source));
			}
		}

		return result;
	}

	@Override
	public Row deserialize(Row reuse, DataInputView source) throws IOException {
		final int len = fieldSerializers.length;

		if (reuse.getArity() != len) {
			throw new RuntimeException("Row arity of from does not match serializers.");
		}

		// read bitmask
		readIntoMask(source, mask);
		if (!legacyModeEnabled) {
			reuse.setKind(readKindFromMask(mask));
		}

		// deserialize fields
		for (int fieldPos = 0; fieldPos < len; fieldPos++) {
			if (mask[(legacyModeEnabled ? 0 : ROW_KIND_OFFSET) + fieldPos]) {
				reuse.setField(fieldPos, null);
			} else {
				Object reuseField = reuse.getField(fieldPos);
				if (reuseField != null) {
					reuse.setField(fieldPos, fieldSerializers[fieldPos].deserialize(reuseField, source));
				}
				else {
					reuse.setField(fieldPos, fieldSerializers[fieldPos].deserialize(source));
				}
			}
		}

		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int len = fieldSerializers.length;

		// copy bitmask
		readIntoAndCopyMask(source, target, mask);

		// copy non-null fields
		for (int fieldPos = 0; fieldPos < len; fieldPos++) {
			if (!mask[ROW_KIND_OFFSET + fieldPos]) {
				fieldSerializers[fieldPos].copy(source, target);
			}
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		RowSerializer that = (RowSerializer) o;
		return legacyModeEnabled == that.legacyModeEnabled &&
			Arrays.equals(fieldSerializers, that.fieldSerializers);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(legacyModeEnabled);
		result = 31 * result + Arrays.hashCode(fieldSerializers);
		return result;
	}

// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshoting & compatibility
	// --------------------------------------------------------------------------------------------

	@Override
	public TypeSerializerSnapshot<Row> snapshotConfiguration() {
		return new RowSerializerSnapshot(this);
	}

	/**
	 * A snapshot for {@link RowSerializer}.
	 *
	 * @deprecated this snapshot class is no longer in use, and is maintained only for backwards compatibility.
	 *             It is fully replaced by {@link RowSerializerSnapshot}.
	 */
	@Deprecated
	public static final class RowSerializerConfigSnapshot extends CompositeTypeSerializerConfigSnapshot<Row> {

		private static final int VERSION = 1;

		/**
		 * This empty nullary constructor is required for deserializing the configuration.
		 */
		public RowSerializerConfigSnapshot() {
		}

		public RowSerializerConfigSnapshot(TypeSerializer<?>[] fieldSerializers) {
			super(fieldSerializers);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		@Override
		public TypeSerializerSchemaCompatibility<Row> resolveSchemaCompatibility(TypeSerializer<Row> newSerializer) {
			TypeSerializerSnapshot<?>[] nestedSnapshots = getNestedSerializersAndConfigs()
				.stream()
				.map(t -> t.f1)
				.toArray(TypeSerializerSnapshot[]::new);

			return CompositeTypeSerializerUtil.delegateCompatibilityCheckToNewSnapshot(
				newSerializer,
				new RowSerializerSnapshot(),
				nestedSnapshots);
		}
	}

	/**
	 * A {@link TypeSerializerSnapshot} for RowSerializer.
	 */
	public static final class RowSerializerSnapshot extends CompositeTypeSerializerSnapshot<Row, RowSerializer> {

		private static final int VERSION = 3;

		private static final int VERSION_WITHOUT_ROW_KIND = 2;

		private boolean legacyModeEnabled = false;

		@SuppressWarnings("WeakerAccess")
		public RowSerializerSnapshot() {
			super(RowSerializer.class);
		}

		RowSerializerSnapshot(RowSerializer serializerInstance) {
			super(serializerInstance);
		}

		@Override
		protected int getCurrentOuterSnapshotVersion() {
			return VERSION;
		}

		@Override
		protected void readOuterSnapshot(
				int readOuterSnapshotVersion,
				DataInputView in,
				ClassLoader userCodeClassLoader) {
			if (readOuterSnapshotVersion == VERSION_WITHOUT_ROW_KIND) {
				legacyModeEnabled = true;
			}
		}

		@Override
		protected TypeSerializer<?>[] getNestedSerializers(RowSerializer outerSerializer) {
			return outerSerializer.fieldSerializers;
		}

		@Override
		protected RowSerializer createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
			return new RowSerializer(nestedSerializers, legacyModeEnabled);
		}
	}
}
