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

package org.apache.flink.table.types.logical;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Logical type of a sequence of fields. A field consists of a field name, field type, and an optional
 * description. The most specific type of a row of a table is a row type. In this case, each column
 * of the row corresponds to the field of the row type that has the same ordinal position as the
 * column. Compared to the SQL standard, an optional field description simplifies the handling with
 * complex structures.
 *
 * <p>The serialized string representation is {@code ROW<n0 t0 'd0', n1 t1 'd1', ...>} where
 * {@code n} is the name of a field, {@code t} is the type of a field, {@code d} is the description
 * of a field.
 */
@PublicEvolving
public final class RowType extends LogicalType {

	private final static String DEFAULT_FORMAT = "ROW<%s>";

	private final static Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
		Row.class.getName(),
		"org.apache.flink.table.dataformat.BaseRow");

	private final static Class<?> DEFAULT_CONVERSION = Row.class;

	/**
	 * Describes a field of a {@link RowType}.
	 */
	public static final class RowField implements Serializable {

		private final static String DEFAULT_FIELD_FORMAT_WITH_DESCRIPTION = "%s %s '%s'";

		private final static String DEFAULT_FIELD_FORMAT_NO_DESCRIPTION = "%s %s";

		private final String name;

		private final LogicalType type;

		private final @Nullable String description;

		public RowField(String name, LogicalType type, @Nullable String description) {
			this.name = Preconditions.checkNotNull(name, "Field name must not be null.");
			this.type = Preconditions.checkNotNull(type, "Field type must not be null.");
			this.description = description;
		}

		public RowField(String name, LogicalType type) {
			this(name, type, null);
		}

		public String getName() {
			return name;
		}

		public LogicalType getType() {
			return type;
		}

		public Optional<String> getDescription() {
			return Optional.ofNullable(description);
		}

		public RowField copy() {
			return new RowField(name, type.copy(), description);
		}

		public String asSummaryString() {
			return formatString(type.asSummaryString());
		}

		public String asSerializableString() {
			return formatString(type.asSerializableString());
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			RowField rowField = (RowField) o;
			return name.equals(rowField.name) &&
				type.equals(rowField.type) &&
				Objects.equals(description, rowField.description);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, type, description);
		}

		private String formatString(String typeString) {
			if (description == null) {
				return String.format(
					DEFAULT_FIELD_FORMAT_NO_DESCRIPTION,
					escapeIdentifier(name),
					typeString);
			} else {
				return String.format(
					DEFAULT_FIELD_FORMAT_WITH_DESCRIPTION,
					escapeIdentifier(name),
					typeString,
					escapeSingleQuotes(description));
			}
		}
	}

	private final List<RowField> fields;

	public RowType(boolean isNullable, List<RowField> fields) {
		super(isNullable, LogicalTypeRoot.ROW);
		this.fields = Collections.unmodifiableList(
			new ArrayList<>(
				Preconditions.checkNotNull(fields, "Fields must not be null.")));
	}

	public RowType(List<RowField> fields) {
		this(true, fields);
	}

	public List<RowField> getFields() {
		return fields;
	}

	@Override
	public LogicalType copy(boolean isNullable) {
		return new RowType(
			isNullable,
			fields.stream().map(RowField::copy).collect(Collectors.toList()));
	}

	@Override
	public String asSummaryString() {
		return withNullability(
			DEFAULT_FORMAT,
			fields.stream()
				.map(RowField::asSummaryString)
				.collect(Collectors.joining(", ")));
	}

	@Override
	public String asSerializableString() {
		return withNullability(
			DEFAULT_FORMAT,
			fields.stream()
				.map(RowField::asSerializableString)
				.collect(Collectors.joining(", ")));
	}

	@Override
	public boolean supportsInputConversion(Class<?> clazz) {
		return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
	}

	@Override
	public boolean supportsOutputConversion(Class<?> clazz) {
		return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
	}

	@Override
	public Class<?> getDefaultOutputConversion() {
		return DEFAULT_CONVERSION;
	}

	@Override
	public List<LogicalType> getChildren() {
		return Collections.unmodifiableList(
			fields.stream()
				.map(RowField::getType)
				.collect(Collectors.toList()));
	}

	@Override
	public <R> R accept(LogicalTypeVisitor<R> visitor) {
		return visitor.visit(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		RowType rowType = (RowType) o;
		return fields.equals(rowType.fields);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), fields);
	}
}
