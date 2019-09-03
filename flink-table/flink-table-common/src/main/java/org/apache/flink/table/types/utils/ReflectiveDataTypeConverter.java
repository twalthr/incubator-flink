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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Reflection-based utility that analyzes a given {@link java.lang.reflect.Type} to extract a (possibly
 * nested) {@link DataType} from it.
 */
@Internal
public final class ReflectiveDataTypeConverter {

	private static final int CURRENT_VERSION = 1;

	private static final int UNDEFINED = -1;

	private final int version;

	private final boolean allowAny;

	private final List<String> anyPatterns;

	private final int defaultDecimalPrecision;

	private final int defaultDecimalScale;

	private final int defaultYearPrecision;

	private final int defaultSecondPrecision;

	private ReflectiveDataTypeConverter(
			int version,
			boolean allowAny,
			List<String> anyPatterns,
			int defaultDecimalPrecision,
			int defaultDecimalScale,
			int defaultYearPrecision,
			int defaultSecondPrecision) {
		this.version = version;
		this.allowAny = allowAny;
		this.anyPatterns = anyPatterns;
		this.defaultDecimalPrecision = defaultDecimalPrecision;
		this.defaultDecimalScale = defaultDecimalScale;
		this.defaultYearPrecision = defaultYearPrecision;
		this.defaultSecondPrecision = defaultSecondPrecision;
	}

	public static Builder newInstance() {
		return new Builder();
	}

	public DataType extractDataType(Class<?> baseClass, int genericPos, Type type) {
		try {
			return extractDataTypeOrAny(baseClass, genericPos, type);
		} catch (Throwable t) {
			throw new ValidationException(
				String.format(
					"Could not extract a data type from %s for %s at position %d.",
					type.toString(),
					baseClass.getName(),
					genericPos),
				t);
		}
	}

	public DataType extractDataType(Type type) {
		try {
			return extractDataTypeOrAny(Collections.singletonList(type), type);
		} catch (Throwable t) {
			throw new ValidationException(
				String.format(
					"Could not extract a data type from %s. Please pass the required data type manually.",
					type.toString()),
				t);
		}
	}

	// --------------------------------------------------------------------------------------------

	private DataType extractDataTypeOrAny(Class<?> baseClass, int genericPos, Type type) {
		final List<Type> typeHierarchy = collectTypeHierarchy(baseClass, type);
		final TypeVariable<?> variable = baseClass.getTypeParameters()[genericPos];
		return extractDataTypeOrError(typeHierarchy, variable);
	}

	private DataType extractDataTypeOrAny(List<Type> typeHierarchy, Type type) {
		try {
			return extractDataTypeOrError(typeHierarchy, type);
		} catch (Throwable t) {
			// ignore the exception and just treat it as ANY type
			if (allowAny) {
				final Class<?> clazz = toClass(type);
				if (clazz == null) {
					return toAnyDataType(Object.class);
				}
				return toAnyDataType(clazz);
			}
			// forward the root cause
			throw t;
		}
	}

	private DataType extractDataTypeOrError(List<Type> typeHierarchy, Type type) {
		// resolve type variables
		if (type instanceof TypeVariable) {
			type = resolveVariable(typeHierarchy, (TypeVariable) type);
		}

		// skip extraction for patterns
		DataType resultDataType = extractAnyPatternType(type);
		if (resultDataType != null) {
			return resultDataType;
		}

		// use annotation-based extraction

		// PREDEFINED
		resultDataType = extractPredefinedType(type);
		if (resultDataType != null) {
			return resultDataType;
		}

		// MAP
		resultDataType = extractMapType(typeHierarchy, type);
		if (resultDataType != null) {
			return resultDataType;
		}

		// ARRAY
		resultDataType = extractArrayType(typeHierarchy, type);
		if (resultDataType != null) {
			return resultDataType;
		}

		// try interpret the type as a STRUCTURED type
		try {
			return extractStructuredType(typeHierarchy, type);
		} catch (Throwable t) {
			throw extractionError(
				t,
				"Unsupported type '%s'. Interpreting it as a structured type was also not successful.",
				type.toString());
		}
	}

	private @Nullable DataType extractAnyPatternType(Type type) {
		if (!anyPatterns.isEmpty()) {
			final Class<?> clazz = toClass(type);
			if (clazz == null) {
				return null;
			}
			// dive into (nested) array types for pattern matching
			if (clazz.isArray()) {
				final DataType elementType = extractAnyPatternType(type);
				if (elementType == null) {
					return null;
				}
				return DataTypes.ARRAY(elementType);
			}
			// for atomic types
			final String className = clazz.getName();
			for (String anyPattern : anyPatterns) {
				if (className.startsWith(anyPattern)) {
					return toAnyDataType(clazz);
				}
			}
		}
		return null;
	}

	private @Nullable DataType extractPredefinedType(Type type) {
		// all predefined types are representable as classes
		if (!(type instanceof Class)) {
			return null;
		}
		final Class<?> clazz = (Class<?>) type;

		// DECIMAL
		if (clazz == BigDecimal.class) {
			if (defaultDecimalPrecision != UNDEFINED && defaultDecimalScale != UNDEFINED) {
				return DataTypes.DECIMAL(defaultDecimalPrecision, defaultDecimalScale);
			} else if (defaultDecimalPrecision != UNDEFINED) {
				return DataTypes.DECIMAL(defaultDecimalPrecision, 0);
			}
			throw extractionError("Values of %s need fixed precision and scale.", BigDecimal.class.getName());
		}

		// TIME
		else if (clazz == java.sql.Time.class || clazz == java.time.LocalTime.class) {
			if (defaultSecondPrecision != UNDEFINED) {
				return DataTypes.TIME(defaultSecondPrecision)
					.bridgedTo(clazz);
			}
		}

		// TIMESTAMP
		else if (clazz == java.sql.Timestamp.class || clazz == java.time.LocalDateTime.class) {
			if (defaultSecondPrecision != UNDEFINED) {
				return DataTypes.TIMESTAMP(defaultSecondPrecision)
					.bridgedTo(clazz);
			}
		}

		// TIMESTAMP WITH TIME ZONE
		else if (clazz == java.time.OffsetDateTime.class) {
			if (defaultSecondPrecision != UNDEFINED) {
				return DataTypes.TIMESTAMP_WITH_TIME_ZONE(defaultSecondPrecision);
			}
		}

		// TIMESTAMP WITH LOCAL TIME ZONE
		else if (clazz == java.time.Instant.class) {
			if (defaultSecondPrecision != UNDEFINED) {
				return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(defaultSecondPrecision);
			}
		}

		// INTERVAL SECOND
		else if (clazz == java.time.Duration.class) {
			if (defaultSecondPrecision != UNDEFINED) {
				return DataTypes.INTERVAL(DataTypes.SECOND(defaultSecondPrecision));
			}
		}

		// INTERVAL YEAR TO MONTH
		else if (clazz == java.time.Period.class) {
			if (defaultYearPrecision == 0) {
				return DataTypes.INTERVAL(DataTypes.MONTH());
			} else if (defaultYearPrecision != UNDEFINED) {
				return DataTypes.INTERVAL(DataTypes.YEAR(defaultYearPrecision), DataTypes.MONTH());
			}
		}

		return ClassDataTypeConverter.extractDataType(clazz).orElse(null);
	}

	private @Nullable DataType extractMapType(List<Type> typeHierarchy, Type type) {
		final Class<?> clazz = toClass(type);
		if (clazz != Map.class) {
			return null;
		}
		if (!(type instanceof ParameterizedType)) {
			throw extractionError("Raw map type needs generic parameters.");
		}
		final ParameterizedType parameterizedType = (ParameterizedType) type;
		final DataType key = extractDataTypeOrAny(
			typeHierarchy,
			parameterizedType.getActualTypeArguments()[0]);
		final DataType value = extractDataTypeOrAny(
			typeHierarchy,
			parameterizedType.getActualTypeArguments()[1]);
		return DataTypes.MAP(key, value);
	}

	private @Nullable DataType extractArrayType(List<Type> typeHierarchy, Type type) {
		// for T[]
		if (type instanceof GenericArrayType) {
			final GenericArrayType genericArray = (GenericArrayType) type;
			return DataTypes.ARRAY(
				extractDataTypeOrAny(typeHierarchy, genericArray.getGenericComponentType()));
		}
		// for my.custom.Pojo[][]
		else if (type instanceof Class) {
			final Class<?> clazz = (Class<?>) type;
			if (clazz.isArray()) {
				return DataTypes.ARRAY(
					extractDataTypeOrAny(typeHierarchy, clazz.getComponentType()));
			}
		}
		return null;
	}

	private DataType extractStructuredType(List<Type> typeHierarchy, Type type) {
		final Class<?> clazz = toClass(type);
		if (clazz == null) {
			throw extractionError("Not a class type.");
		}
		validateStructuredClass(clazz);
		final List<Field> fields = collectStructuredFields(clazz);

		return null;
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	private static void validateStructuredClass(Class<?> clazz) {
		final int m = clazz.getModifiers();
		if (Modifier.isAbstract(m)) {
			throw extractionError("Class %s must not be abstract.", clazz.getName());
		}
		if (!Modifier.isPublic(m)) {
			throw extractionError("Class %s is not public.", clazz.getName());
		}
		if (clazz.getEnclosingClass() != null &&
				(clazz.getDeclaringClass() == null || !Modifier.isStatic(m))) {
			throw extractionError("Class %s is a not a static, globally accessible class.", clazz.getName());
		}
	}

	private static @Nullable Class<?> toClass(Type type) {
		if (type instanceof Class) {
			return (Class<?>) type;
		} else if (type instanceof ParameterizedType) {
			// this is always a class
			return (Class<?>) ((ParameterizedType) type).getRawType();
		}
		// unsupported: generic arrays, type variables, wildcard types
		return null;
	}

	private static DataType toAnyDataType(Class<?> clazz) {
		return DataTypes.ANY(new GenericTypeInfo<>(clazz));
	}

	private static List<Type> collectTypeHierarchy(Class<?> baseClass, Type type) {
		Type currentType = type;
		Class<?> currentClass = toClass(type);
		if (currentClass == null || !baseClass.isAssignableFrom(currentClass)) {
			throw extractionError(
				"Base class %s is not a super class of type %s.",
				baseClass.getName(),
				type.toString());
		}
		final List<Type> typeHierarchy = new ArrayList<>();
		while (currentClass != baseClass) {
			assert currentClass != null;
			typeHierarchy.add(currentType);
			currentType = currentClass.getGenericSuperclass();
			currentClass = toClass(currentType);
		}
		typeHierarchy.add(currentType);
		return typeHierarchy;
	}

	private static List<Field> collectStructuredFields(Class<?> clazz) {
		final Field[] fields = clazz.getDeclaredFields();
		return Stream.of(fields)
			.filter(field -> {
				final int m = field.getModifiers();
				return !Modifier.isStatic(m) && !Modifier.isTransient(m);
			})
			.collect(Collectors.toList());
	}

	private static Type resolveVariable(List<Type> typeHierarchy, TypeVariable<?> variable) {
		// iterate through hierarchy from top to bottom until type variable gets a non-variable assigned
		for (int i = typeHierarchy.size() - 1; i >= 0; i--) {
			final Type currentType = typeHierarchy.get(i);
			if (currentType instanceof ParameterizedType) {
				final ParameterizedType currentParameterized = (ParameterizedType) currentType;
				final Class<?> currentRaw = (Class<?>) currentParameterized.getRawType();
				final TypeVariable<?>[] currentVariables = currentRaw.getTypeParameters();
				// search for matching type variable
				for (int paramPos = 0; paramPos < currentVariables.length; paramPos++) {
					final TypeVariable<?> currentVariable = currentVariables[paramPos];
					if (currentVariable.getGenericDeclaration().equals(variable.getGenericDeclaration()) &&
							currentVariable.getName().equals(variable.getName())) {
						final Type resolvedType = currentParameterized.getActualTypeArguments()[paramPos];
						// follow type variables transitively
						if (resolvedType instanceof TypeVariable) {
							variable = (TypeVariable<?>) resolvedType;
						} else {
							return resolvedType;
						}
					}
				}
			}
		}
		// unresolved variable
		return variable;
	}

	private static IllegalStateException extractionError(Throwable t, String cause, Object... args) {
		return new IllegalStateException(
			String.format(cause, args),
			t
		);
	}

	private static IllegalStateException extractionError(String cause, Object... args) {
		return extractionError(null, cause, args);
	}




























































	@Override
	public String toString() {
		return String.format(
			"%s(version=%d, allowAny=%b, defaultDecimalPrecision=%d, defaultDecimalScale=%d, defaultSecondPrecision=%d)",
			getClass().getName(), version, allowAny, defaultDecimalPrecision, defaultDecimalScale, defaultSecondPrecision);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Builder for creating an instance of {@link ReflectiveDataTypeConverter}.
	 */
	public static final class Builder {

		private int version = ReflectiveDataTypeConverter.CURRENT_VERSION;

		private boolean allowAny = false;

		private List<String> anyPatterns = Collections.emptyList();

		private int defaultDecimalPrecision = UNDEFINED;

		private int defaultDecimalScale = UNDEFINED;

		private int defaultYearPrecision = UNDEFINED;

		private int defaultSecondPrecision = UNDEFINED;

		public Builder() {
			// default constructor for fluent definition
		}

		/**
		 * Logic version for future backwards compatibility. Current version by default.
		 */
		public Builder version(int version) {
			this.version = version;
			return this;
		}

		/**
		 * Defines whether {@link DataTypes#ANY(TypeInformation)} should be used for classes that
		 * cannot be mapped to any SQL-like type. Set to {@code false} by default, which means that
		 * an exception is thrown for unmapped types.
		 *
		 * <p>For example, {@link java.math.BigDecimal} cannot be mapped because the SQL standard
		 * defines that decimals have a fixed precision and scale.
		 */
		public Builder allowAny(boolean allowAny) {
			this.allowAny = allowAny;
			return this;
		}

		/**
		 * Patterns that force the usage of an ANY type. A pattern is a prefix or a fully qualified
		 * name of {@link Class#getName()} excluding arrays.
		 */
		public Builder anyPatterns(List<String> anyPatterns) {
			this.anyPatterns = anyPatterns;
			return this;
		}

		/**
		 * Sets a default precision and scale for all decimals that occur. By default, decimals are
		 * not extracted.
		 */
		public Builder defaultDecimal(int precision, int scale) {
			this.defaultDecimalPrecision = precision;
			this.defaultDecimalScale = scale;
			return this;
		}

		/**
		 * Sets a default year precision for year-month intervals. If set to 0, a month interval is
		 * assumed.
		 *
		 * @see ClassDataTypeConverter
		 */
		public Builder defaultYearPrecision(int precision) {
			this.defaultYearPrecision = precision;
			return this;
		}

		/**
		 * Sets a default second fraction for timestamps and intervals that occur.
		 *
		 * @see ClassDataTypeConverter
		 */
		public Builder defaultSecondPrecision(int precision) {
			this.defaultSecondPrecision = precision;
			return this;
		}

		public ReflectiveDataTypeConverter build() {
			return new ReflectiveDataTypeConverter(
				version,
				allowAny,
				anyPatterns,
				defaultDecimalPrecision,
				defaultDecimalScale,
				defaultYearPrecision,
				defaultSecondPrecision);
		}
	}
}
