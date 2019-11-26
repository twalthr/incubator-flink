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

package org.apache.flink.table.types.extraction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeLookup;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.extraction.utils.DataTypeTemplate;
import org.apache.flink.table.types.extraction.utils.ExtractionUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.utils.ClassDataTypeConverter;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.collectAnnotationsOfField;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.collectStructuredFields;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.collectTypeHierarchy;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.createRawType;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.extractAssigningConstructor;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.extractionError;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.isStructuredFieldMutable;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.resolveVariable;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.toClass;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.validateStructuredClass;
import static org.apache.flink.table.types.extraction.utils.ExtractionUtils.validateStructuredFieldReadability;

/**
 * Reflection-based utility that analyzes a given {@link java.lang.reflect.Type} to extract a (possibly
 * nested) {@link DataType} from it.
 */
@Internal
public final class DataTypeExtractor {

	private final DataTypeLookup lookup;

	private DataTypeExtractor(DataTypeLookup lookup) {
		this.lookup = lookup;
	}

	public static DataType extractFromGeneric(
			DataTypeLookup lookup,
			DataTypeTemplate template,
			Class<?> baseClass,
			int genericPos,
			Type type) {
		try {
			final DataTypeExtractor extractor = new DataTypeExtractor(lookup);
			return extractor.extractDataTypeOrRaw(template, baseClass, genericPos, type);
		} catch (Throwable t) {
			throw extractionError(
				t,
				"Could not extract a data type from '%s' for '%s' at position %d. " +
					"Please pass the required data type manually or allow RAW types.",
				type.toString(),
				baseClass.getName(),
				genericPos);
		}
	}

	public static DataType extractFromType(
			DataTypeLookup lookup,
			DataTypeTemplate template,
			Type type) {
		try {
			final DataTypeExtractor extractor = new DataTypeExtractor(lookup);
			return extractor.extractDataTypeOrRaw(template, Collections.singletonList(type), type);
		} catch (Throwable t) {
			throw extractionError(
				t,
				"Could not extract a data type from '%s'. " +
					"Please pass the required data type manually or allow RAW types.",
				type.toString());
		}
	}

	public static DataType extractFromMethod(
			DataTypeLookup lookup,
			DataTypeTemplate template,
			Method method,
			int paramPos) {
		throw new ValidationException("Unsupported.");
	}

	// --------------------------------------------------------------------------------------------

	private DataType extractDataTypeOrRaw(
			DataTypeTemplate template,
			Class<?> baseClass,
			int genericPos,
			Type type) {
		final List<Type> typeHierarchy = collectTypeHierarchy(baseClass, type);
		final TypeVariable<?> variable = baseClass.getTypeParameters()[genericPos];
		return extractDataTypeOrError(template, typeHierarchy, variable);
	}

	private DataType extractDataTypeOrRaw(DataTypeTemplate template, List<Type> typeHierarchy, Type type) {
		// template defines a data type
		if (template.hasDataTypeDefinition()) {
			return template.dataType;
		}

		// best effort resolution of type variables, the resolved type can still be a variable
		final Type resolvedType;
		if (type instanceof TypeVariable) {
			resolvedType = resolveVariable(typeHierarchy, (TypeVariable) type);
		} else {
			resolvedType = type;
		}

		try {
			return extractDataTypeOrError(template, typeHierarchy, resolvedType);
		} catch (Throwable t) {
			// ignore the exception and just treat it as RAW type
			final Class<?> clazz = toClass(resolvedType);
			if (isAllowRawGlobally(template) || isAllowAnyPattern(template, clazz)) {
				return createRawType(lookup, template.rawSerializer, clazz);
			}
			// forward the root cause
			throw t;
		}
	}

	private DataType extractDataTypeOrError(DataTypeTemplate template, List<Type> typeHierarchy, Type type) {
		// still a type variable
		if (type instanceof TypeVariable) {
			throw extractionError(
				"Unresolved type variable '%s'. A data type cannot be extracted from a type variable. " +
					"The original content might have been erased due to Java type erasure.",
				type.toString());
		}

		// ARRAY
		DataType resultDataType = extractArrayType(template, typeHierarchy, type);
		if (resultDataType != null) {
			return resultDataType;
		}

		// skip extraction for enforced patterns early but after arrays
		resultDataType = extractEnforcedRawType(template, type);
		if (resultDataType != null) {
			return resultDataType;
		}

		// PREDEFINED
		resultDataType = extractPredefinedType(template, type);
		if (resultDataType != null) {
			return resultDataType;
		}

		// MAP
		resultDataType = extractMapType(template, typeHierarchy, type);
		if (resultDataType != null) {
			return resultDataType;
		}

		// try interpret the type as a STRUCTURED type
		try {
			return extractStructuredType(typeHierarchy, type);
		} catch (Throwable t) {
			throw extractionError(
				t,
				"Could not extract a data type from '%s'. " +
					"Interpreting it as a structured type was also not successful.",
				type.toString());
		}
	}

	private @Nullable DataType extractArrayType(
			DataTypeTemplate template,
			List<Type> typeHierarchy,
			Type type) {
		// for T[]
		if (type instanceof GenericArrayType) {
			final GenericArrayType genericArray = (GenericArrayType) type;
			return DataTypes.ARRAY(
				extractDataTypeOrRaw(template, typeHierarchy, genericArray.getGenericComponentType()));
		}
		// for my.custom.Pojo[][]
		else if (type instanceof Class) {
			final Class<?> clazz = (Class<?>) type;
			if (clazz.isArray()) {
				return DataTypes.ARRAY(
					extractDataTypeOrRaw(template, typeHierarchy, clazz.getComponentType()));
			}
		}
		return null;
	}

	private @Nullable DataType extractEnforcedRawType(DataTypeTemplate template, Type type) {
		final Class<?> clazz = toClass(type);
		if (template.forceRawPattern == null || clazz == null) {
			return null;
		}
		final String className = clazz.getName();
		for (String anyPattern : template.forceRawPattern) {
			if (className.startsWith(anyPattern)) {
				return createRawType(lookup, template.rawSerializer, clazz);
			}
		}
		return null;
	}

	private @Nullable DataType extractPredefinedType(DataTypeTemplate template, Type type) {
		final Class<?> clazz = toClass(type);
		// all predefined types are representable as classes
		if (clazz == null) {
			return null;
		}

		// DECIMAL
		if (clazz == BigDecimal.class) {
			if (template.defaultDecimalPrecision != null && template.defaultDecimalScale != null) {
				return DataTypes.DECIMAL(template.defaultDecimalPrecision, template.defaultDecimalScale);
			} else if (template.defaultDecimalPrecision != null) {
				return DataTypes.DECIMAL(template.defaultDecimalPrecision, 0);
			}
			throw extractionError("Values of '%s' need fixed precision and scale.", BigDecimal.class.getName());
		}

		// TIME
		else if (clazz == java.sql.Time.class || clazz == java.time.LocalTime.class) {
			if (template.defaultSecondPrecision != null) {
				return DataTypes.TIME(template.defaultSecondPrecision)
					.bridgedTo(clazz);
			}
		}

		// TIMESTAMP
		else if (clazz == java.sql.Timestamp.class || clazz == java.time.LocalDateTime.class) {
			if (template.defaultSecondPrecision != null) {
				return DataTypes.TIMESTAMP(template.defaultSecondPrecision)
					.bridgedTo(clazz);
			}
		}

		// TIMESTAMP WITH TIME ZONE
		else if (clazz == java.time.OffsetDateTime.class) {
			if (template.defaultSecondPrecision != null) {
				return DataTypes.TIMESTAMP_WITH_TIME_ZONE(template.defaultSecondPrecision);
			}
		}

		// TIMESTAMP WITH LOCAL TIME ZONE
		else if (clazz == java.time.Instant.class) {
			if (template.defaultSecondPrecision != null) {
				return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(template.defaultSecondPrecision);
			}
		}

		// INTERVAL SECOND
		else if (clazz == java.time.Duration.class) {
			if (template.defaultSecondPrecision != null) {
				return DataTypes.INTERVAL(DataTypes.SECOND(template.defaultSecondPrecision));
			}
		}

		// INTERVAL YEAR TO MONTH
		else if (clazz == java.time.Period.class) {
			if (template.defaultYearPrecision != null && template.defaultYearPrecision == 0) {
				return DataTypes.INTERVAL(DataTypes.MONTH());
			} else if (template.defaultYearPrecision != null) {
				return DataTypes.INTERVAL(DataTypes.YEAR(template.defaultYearPrecision), DataTypes.MONTH());
			}
		}

		return ClassDataTypeConverter.extractDataType(clazz).orElse(null);
	}

	private @Nullable DataType extractMapType(DataTypeTemplate template, List<Type> typeHierarchy, Type type) {
		final Class<?> clazz = toClass(type);
		if (clazz != Map.class) {
			return null;
		}
		if (!(type instanceof ParameterizedType)) {
			throw extractionError("Raw map type needs generic parameters.");
		}
		final ParameterizedType parameterizedType = (ParameterizedType) type;
		final DataType key = extractDataTypeOrRaw(
			template,
			typeHierarchy,
			parameterizedType.getActualTypeArguments()[0]);
		final DataType value = extractDataTypeOrRaw(
			template,
			typeHierarchy,
			parameterizedType.getActualTypeArguments()[1]);
		return DataTypes.MAP(key, value);
	}

	private DataType extractStructuredType(DataTypeTemplate template, List<Type> typeHierarchy, Type type) {
		final Class<?> clazz = toClass(type);
		if (clazz == null) {
			throw extractionError("Not a class type.");
		}

		validateStructuredClass(clazz);

		final List<Field> fields = collectStructuredFields(clazz);

		boolean requireAssigningConstructor = false;
		for (Field field : fields) {
			validateStructuredFieldReadability(clazz, field);
			final boolean isMutable = isStructuredFieldMutable(clazz, field);
			// not all fields are mutable, a default constructor is not enough
			if (!isMutable) {
				requireAssigningConstructor = true;
			}
		}

		final ExtractionUtils.AssigningConstructor constructor = extractAssigningConstructor(clazz, fields);
		if (requireAssigningConstructor && constructor == null) {
			throw extractionError(
				"Class '%s' has immutable fields and thus requires a constructor that is publicly " +
					"accessible and assigns all fields: %s",
				clazz.getName(),
				fields.stream().map(Field::getName).collect(Collectors.joining(", ")));
		}

		final Map<String, DataType> fieldDataTypes = new HashMap<>();
		final List<Type> structuredTypeHierarchy = collectTypeHierarchy(Object.class, type);
		for (Field field : fields) {
			final Type fieldType = field.getGenericType();
			final List<Type> fieldTypeHierarchy = new ArrayList<>();
			// hierarchy until structured type
			fieldTypeHierarchy.addAll(typeHierarchy);
			// hierarchy of structured type
			fieldTypeHierarchy.addAll(structuredTypeHierarchy);
			// field type
			fieldTypeHierarchy.add(fieldType);
			final DataTypeTemplate fieldTemplate = mergeFieldTemplate(lookup, field, template);
			final DataType fieldDataType = extractDataTypeOrRaw(fieldTemplate, fieldTypeHierarchy, fieldType);
			fieldDataTypes.put(field.getName(), fieldDataType);
		}

		final List<StructuredType.StructuredAttribute> attributes;
		// field order is defined by assigning constructor
		if (constructor != null) {
			attributes = constructor.parameterNames
				.stream()
				.map(name -> {
					final LogicalType logicalType = fieldDataTypes.get(name).getLogicalType();
					return new StructuredType.StructuredAttribute(name, logicalType);
				})
				.collect(Collectors.toList());
		}
		// field order is sorted
		else {
			attributes = fieldDataTypes.keySet()
				.stream()
				.sorted()
				.map(name -> {
					final LogicalType logicalType = fieldDataTypes.get(name).getLogicalType();
					return new StructuredType.StructuredAttribute(name, logicalType);
				})
				.collect(Collectors.toList());
		}

		final StructuredType.Builder builder = StructuredType.newInstance(clazz);
		builder.attributes(attributes);
		builder.isFinal(true); // anonymous structured types should not allow inheritance
		builder.isInstantiable(true);
		return new FieldsDataType(builder.build(), clazz, fieldDataTypes);
	}

	private DataTypeTemplate mergeFieldTemplate(DataTypeLookup lookup, Field field, DataTypeTemplate structuredTemplate) {
		final Set<DataTypeHint> hints = collectAnnotationsOfField(DataTypeHint.class, field);
		if (hints.size() != 1) {
			return structuredTemplate.copyWithErasedDataType();
		}
		final DataTypeHint hint = hints.iterator().next();
		return structuredTemplate.mergeWithAnnotation(lookup, hint);
	}

	// --------------------------------------------------------------------------------------------

	private static boolean isAllowRawGlobally(DataTypeTemplate template) {
		return template.allowRawGlobally != null && template.allowRawGlobally;
	}

	private static boolean isAllowAnyPattern(DataTypeTemplate template, @Nullable Class<?> clazz) {
		if (template.allowRawPattern == null || clazz == null) {
			return false;
		}
		for (String pattern : template.allowRawPattern) {
			if (clazz.getName().startsWith(pattern)) {
				return true;
			}
		}
		return false;
	}
}
