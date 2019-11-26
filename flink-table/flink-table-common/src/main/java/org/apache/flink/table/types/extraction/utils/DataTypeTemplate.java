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

package org.apache.flink.table.types.extraction.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ExtractionVersion;
import org.apache.flink.table.annotation.HintFlag;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeLookup;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.extraction.DataTypeExtractor;
import org.apache.flink.table.types.inference.InputTypeValidator;
import org.apache.flink.table.types.inference.InputTypeValidators;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.inference.validators.SingleInputTypeValidator;

import javax.annotation.Nullable;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.createRawType;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.extractionError;

/**
 * Template of a {@link DataType} or {@link InputTypeValidator}.
 */
@Internal
public final class DataTypeTemplate {

	private static final DataTypeHint DEFAULT_ANNOTATION = getDefaultAnnotation();

	private static final String RAW_TYPE_NAME = "RAW";

	public final @Nullable DataType dataType;

	public final @Nullable Class<?> rawSerializer;

	public final @Nullable InputGroup inputGroup;

	public final @Nullable ExtractionVersion version;

	public final @Nullable Boolean allowRawGlobally;

	public final @Nullable String[] allowRawPattern;

	public final @Nullable String[] forceRawPattern;

	public final @Nullable Integer defaultDecimalPrecision;

	public final @Nullable Integer defaultDecimalScale;

	public final @Nullable Integer defaultYearPrecision;

	public final @Nullable Integer defaultSecondPrecision;

	private DataTypeTemplate(
			@Nullable DataType dataType,
			@Nullable Class<?> rawSerializer,
			@Nullable InputGroup inputGroup,
			@Nullable ExtractionVersion version,
			@Nullable Boolean allowRawGlobally,
			@Nullable String[] allowRawPattern,
			@Nullable String[] forceRawPattern,
			@Nullable Integer defaultDecimalPrecision,
			@Nullable Integer defaultDecimalScale,
			@Nullable Integer defaultYearPrecision,
			@Nullable Integer defaultSecondPrecision) {
		this.dataType = dataType;
		this.rawSerializer = rawSerializer;
		this.inputGroup = inputGroup;
		this.version = version;
		this.allowRawGlobally = allowRawGlobally;
		this.allowRawPattern = allowRawPattern;
		this.forceRawPattern = forceRawPattern;
		this.defaultDecimalPrecision = defaultDecimalPrecision;
		this.defaultDecimalScale = defaultDecimalScale;
		this.defaultYearPrecision = defaultYearPrecision;
		this.defaultSecondPrecision = defaultSecondPrecision;
	}

	public static DataTypeTemplate extractFromMethod(DataTypeLookup lookup, Method method) {
		throw new UnsupportedOperationException(); // TODO
	}

	public static DataTypeTemplate fromAnnotation(DataTypeLookup lookup, DataTypeHint hint) {
		final String typeName = defaultAsNull(hint, DataTypeHint::value);
		final Class<?> conversionClass = defaultAsNull(hint, DataTypeHint::bridgedTo);
		if (typeName != null || conversionClass != null) {
			final DataTypeTemplate extractionTemplate = fromAnnotation(hint, null);
			return fromAnnotation(hint, extractDataType(lookup, typeName, conversionClass, extractionTemplate));
		}
		return fromAnnotation(hint, null);
	}

	public static DataTypeTemplate fromAnnotation(DataTypeHint hint, @Nullable DataType dataType) {
		return new DataTypeTemplate(
			dataType,
			defaultAsNull(hint, DataTypeHint::rawSerializer),
			defaultAsNull(hint, DataTypeHint::inputGroup),
			defaultAsNull(hint, DataTypeHint::version),
			hintFlagToBoolean(defaultAsNull(hint, DataTypeHint::allowRawGlobally)),
			defaultAsNull(hint, DataTypeHint::allowRawPattern),
			defaultAsNull(hint, DataTypeHint::forceRawPattern),
			defaultAsNull(hint, DataTypeHint::defaultDecimalPrecision),
			defaultAsNull(hint, DataTypeHint::defaultDecimalScale),
			defaultAsNull(hint, DataTypeHint::defaultYearPrecision),
			defaultAsNull(hint, DataTypeHint::defaultSecondPrecision)
		);
	}

	public DataTypeTemplate copyWithErasedDataType() {
		return new DataTypeTemplate(
			null,
			rawSerializer,
			inputGroup,
			version,
			allowRawGlobally,
			allowRawPattern,
			forceRawPattern,
			defaultDecimalPrecision,
			defaultDecimalScale,
			defaultYearPrecision,
			defaultSecondPrecision
		);
	}

	public DataTypeTemplate mergeWithAnnotation(DataTypeLookup lookup, DataTypeHint hint) {
		final DataTypeTemplate otherTemplate = fromAnnotation(lookup, hint);
		return new DataTypeTemplate(
			otherTemplate.dataType,
			rightValueIfNotNull(rawSerializer, otherTemplate.rawSerializer),
			rightValueIfNotNull(inputGroup, otherTemplate.inputGroup),
			rightValueIfNotNull(version, otherTemplate.version),
			rightValueIfNotNull(allowRawGlobally, otherTemplate.allowRawGlobally),
			rightValueIfNotNull(allowRawPattern, otherTemplate.allowRawPattern),
			rightValueIfNotNull(forceRawPattern, otherTemplate.forceRawPattern),
			rightValueIfNotNull(defaultDecimalPrecision, otherTemplate.defaultDecimalPrecision),
			rightValueIfNotNull(defaultDecimalScale, otherTemplate.defaultDecimalScale),
			rightValueIfNotNull(defaultYearPrecision, otherTemplate.defaultYearPrecision),
			rightValueIfNotNull(defaultSecondPrecision, otherTemplate.defaultSecondPrecision)
		);
	}

	public boolean hasDataTypeDefinition() {
		return dataType != null;
	}

	public boolean hasInputGroupDefinition() {
		return inputGroup != null && inputGroup != InputGroup.UNKNOWN;
	}

	public SingleInputTypeValidator toSingleInputTypeValidator() {
		// data type
		if (hasDataTypeDefinition()) {
			return InputTypeValidators.explicit(dataType);
		}
		// input group
		else if (hasInputGroupDefinition()) {
			if (inputGroup == InputGroup.ANY) {
				return InputTypeValidators.ANY;
			}
		}
		throw ExtractionUtils.extractionError(
			"Data type hint does neither specify an explicit data type nor an input group.");
	}

	public TypeStrategy toTypeStrategy() {
		return TypeStrategies.explicit(dataType);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		DataTypeTemplate that = (DataTypeTemplate) o;
		return Objects.equals(dataType, that.dataType) &&
			Objects.equals(rawSerializer, that.rawSerializer) &&
			Objects.equals(inputGroup, that.inputGroup) &&
			version == that.version &&
			Objects.equals(allowRawGlobally, that.allowRawGlobally) &&
			Arrays.equals(allowRawPattern, that.allowRawPattern) &&
			Arrays.equals(forceRawPattern, that.forceRawPattern) &&
			Objects.equals(defaultDecimalPrecision, that.defaultDecimalPrecision) &&
			Objects.equals(defaultDecimalScale, that.defaultDecimalScale) &&
			Objects.equals(defaultYearPrecision, that.defaultYearPrecision) &&
			Objects.equals(defaultSecondPrecision, that.defaultSecondPrecision);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(
			dataType,
			rawSerializer,
			inputGroup,
			version,
			allowRawGlobally,
			defaultDecimalPrecision,
			defaultDecimalScale,
			defaultYearPrecision,
			defaultSecondPrecision);
		result = 31 * result + Arrays.hashCode(allowRawPattern);
		result = 31 * result + Arrays.hashCode(forceRawPattern);
		return result;
	}

	// --------------------------------------------------------------------------------------------

	@DataTypeHint
	private static class DefaultAnnotationHelper {
		// no implementation
	}

	private static DataTypeHint getDefaultAnnotation() {
		return DefaultAnnotationHelper.class.getAnnotation(DataTypeHint.class);
	}

	private static <T> T defaultAsNull(DataTypeHint hint, Function<DataTypeHint, T> accessor) {
		final T defaultValue = accessor.apply(DEFAULT_ANNOTATION);
		final T actualValue = accessor.apply(hint);
		if (Objects.deepEquals(defaultValue, actualValue)) {
			return null;
		}
		return actualValue;
	}

	public static <T> T rightValueIfNotNull(T l, T r) {
		if (r != null) {
			return r;
		}
		return l;
	}

	private static Boolean hintFlagToBoolean(HintFlag flag) {
		if (flag == null) {
			return null;
		}
		return flag == HintFlag.TRUE;
	}

	private static DataType extractDataType(
			DataTypeLookup lookup,
			@Nullable String typeName,
			@Nullable Class<?> conversionClass,
			DataTypeTemplate template) {
		// explicit data type
		if (typeName != null) {
			// RAW type
			if (typeName.equals(RAW_TYPE_NAME)) {
				return createRawType(lookup, template.rawSerializer, conversionClass);
			}
			// regular type that must be resolvable
			return lookup.lookupDataType(typeName)
				.map(dataType -> {
					if (conversionClass != null) {
						dataType.bridgedTo(conversionClass);
					}
					return dataType;
				})
				.orElseGet(() -> {
					throw extractionError("Could not resolve type with name '%s'.", typeName);
				});
		}
		// extracted data type
		else if (conversionClass != null) {
			return DataTypeExtractor.extractFromType(template, conversionClass);
		}
		throw ExtractionUtils.extractionError("Data type hint does not specify a data type.");
	}
}
