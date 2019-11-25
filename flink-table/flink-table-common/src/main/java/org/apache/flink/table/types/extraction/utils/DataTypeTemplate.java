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
import org.apache.flink.table.types.inference.InputTypeValidators;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.inference.validators.SingleInputTypeValidator;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.extractionError;

/**
 * Internal representation of a {@link DataTypeHint} with more contextual information used to produce
 * the final {@link DataType}.
 */
@Internal
public final class DataTypeTemplate {

	private static final DataTypeHint DEFAULT_ANNOTATION = getDefaultAnnotation();

	private static final String RAW_TYPE_NAME = "RAW";

	public final @Nullable String typeName;

	public final @Nullable Class<?> conversionClass;

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
			@Nullable String typeName,
			@Nullable Class<?> conversionClass,
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
		this.typeName = typeName;
		this.conversionClass = conversionClass;
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

	public static DataTypeTemplate fromAnnotation(DataTypeHint hint) {
		return new DataTypeTemplate(
			defaultAsNull(hint, DataTypeHint::value),
			defaultAsNull(hint, DataTypeHint::bridgedTo),
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

	public Optional<DataType> toDataType(DataTypeLookup lookup) {
		// no explicit data type
		if (typeName == null) {
			return Optional.empty();
		}
		// RAW type
		if (typeName.equals(RAW_TYPE_NAME)) {
			return Optional.of(createRawType(lookup));
		}
		// regular type that must be resolvable
		final DataType resolvedType = lookup.lookupDataType(typeName)
			.map(dataType -> {
				if (conversionClass != null) {
					dataType.bridgedTo(conversionClass);
				}
				return dataType;
			})
			.orElseGet(() -> {
				throw extractionError("Could not resolve type with name '%s'.", typeName);
			});
		return Optional.of(resolvedType);
	}

	public SingleInputTypeValidator toSingleInputTypeValidator(DataTypeLookup lookup) {
		if (inputGroup == null) {
			final Optional<DataType> explicitDataType = toDataType(lookup);
			return explicitDataType
				.map(InputTypeValidators::explicit)
				.orElseThrow(() ->ExtractionUtils.extractionError(
					"Data type hint does neither specify an explicit data type nor an input group."));
		}
		switch (inputGroup) {
			case ANY:
				return InputTypeValidators.ANY;
			case UNKNOWN:
			default:
				throw new IllegalStateException("Unsupported input group.");
		}
	}

	public TypeStrategy toTypeStrategy(DataTypeLookup lookup) {
		final Optional<DataType> explicitDataType = toDataType(lookup);
		return explicitDataType
			.map(TypeStrategies::explicit)
			.orElseThrow(() ->ExtractionUtils.extractionError(
				"Data type hint does not specify an explicit data type."));
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
		return Objects.equals(typeName, that.typeName) &&
			Objects.equals(conversionClass, that.conversionClass) &&
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
			typeName,
			conversionClass,
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

	private Class<?> createConversionClass() {
		if (conversionClass != null) {
			return conversionClass;
		}
		return Object.class;
	}

	@SuppressWarnings("unchecked")
	private DataType createRawType(DataTypeLookup lookup) {
		if (rawSerializer != null) {
			return DataTypes.ANY((Class) createConversionClass(), instantiateRawSerializer(rawSerializer));
		}
		return lookup.resolveRawDataType(createConversionClass());
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

	private static Boolean hintFlagToBoolean(HintFlag flag) {
		if (flag == null) {
			return null;
		}
		return flag == HintFlag.TRUE;
	}

	private static TypeSerializer<?> instantiateRawSerializer(Class<?> rawSerializer) {
		if (!TypeSerializer.class.isAssignableFrom(rawSerializer)) {
			throw extractionError(
				"Defined class '%s' for RAW serializer does not extend '%s'.",
				rawSerializer.getName(),
				TypeSerializer.class.getName()
			);
		}
		try {
			return (TypeSerializer<?>) rawSerializer.newInstance();
		} catch (Exception e) {
			throw extractionError(
				e,
				"Cannot instantiate type serializer '%s' for RAW type. " +
					"Make sure the class is publicly accessible and has a default constructor",
				rawSerializer.getName()
			);
		}
	}
}
