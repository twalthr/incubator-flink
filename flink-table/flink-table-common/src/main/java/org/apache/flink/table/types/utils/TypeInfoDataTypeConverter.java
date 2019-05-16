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
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.MultisetTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.AnyType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;
import org.apache.flink.table.types.logical.StructuredType.StructuredComparision;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TypeInformationAnyType;
import org.apache.flink.table.types.logical.UserDefinedType.TypeIdentifier;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo;
import org.apache.flink.table.utils.ReflectionUtils;
import org.apache.flink.types.Row;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Converter that converts between supported {@link TypeInformation} and {@link DataType}.
 *
 * <p>The ugliness of this class represents the need for reworking {@link TypeInformation} in flink-core.
 *
 * <p>The conversion is not a 1:1 mapping. A conversion from type information to data type is more complete
 * then the reverse operation. Especially POJOs, tuples, and case classes remain unresolved and
 * are returned as instances of {@link StructuredTypeInformation} when converting a data type to the
 * type information stack.
 */
@Internal
public class TypeInfoDataTypeConverter {

	/**
	 * Java's {@link java.math.BigDecimal} has a variable precision whereas {@link DecimalType}
	 * defines a fixed value.
	 */
	private static final int POST_1_9_DECIMAL_PRECISION = DecimalType.MAX_PRECISION;

	/**
	 * Java's {@link java.math.BigDecimal} has a variable scale whereas {@link DecimalType}
	 * defines a fixed value.
	 */
	private static final int POST_1_9_DECIMAL_SCALE = 18;

	/**
	 * Time precision is no problem as {@link java.sql.Time} does not define sub-seconds.
	 */
	private static final int TIME_PRECISION = 0;

	/**
	 * Before Flink 1.9 the Table & SQL API did only support millisecond precision. However, the type
	 * information and serializer supports that. We convert to a precision of 9 to properly integrate
	 * this type for the future.
	 */
	private static final int POST_1_9_TIMESTAMP_PRECISION = 9;

	/**
	 * Before Flink 1.9 the Table & SQL API did only support millisecond precision.
	 */
	private static final int PRE_1_9_DAY_TIME_INTERVAL_PRECISION = 3;

	/**
	 * Before Flink 1.9 the Table & SQL API did only support millisecond precision.
	 */
	private static final int PRE_1_9_TIME_ATTRIBUTE_PRECISION = 3;

	public static DataType toDataType(TypeInformation<?> typeInfo, boolean withBridging) {
		return convertToDataType(typeInfo, withBridging);
	}

	public static DataType toDataType(TypeInformation<?> typeInfo) {
		return toDataType(typeInfo, true);
	}

	public static DataType[] toDataType(TypeInformation<?>[] typeInfo, boolean withBridging) {
		return Stream.of(typeInfo)
			.map(ti -> toDataType(ti, withBridging))
			.toArray(DataType[]::new);
	}

	public static DataType[] toDataType(TypeInformation<?>[] typeInfo) {
		return toDataType(typeInfo, true);
	}

	public static TypeInformation<?> toTypeInfo(DataType dataType) {
		return convertToTypeInfo(dataType);
	}

	public static TypeInformation<?>[] toTypeInfo(DataType[] dataTypes) {
		return Stream.of(dataTypes)
			.map(TypeInfoDataTypeConverter::toTypeInfo)
			.toArray(TypeInformation<?>[]::new);
	}

	// --------------------------------------------------------------------------------------------
	// From TypeInformation to DataType
	// --------------------------------------------------------------------------------------------

	private static DataType convertToDataType(TypeInformation<?> typeInfo, boolean withBridging) {
		final DataType dataType = convertToLogicalType(typeInfo, withBridging);
		if (withBridging) {
			return dataType.bridgedTo(typeInfo.getTypeClass());
		}
		return dataType;
	}

	private static DataType convertToLogicalType(TypeInformation<?> typeInfo, boolean withBridging) {

		if (typeInfo.equals(Types.STRING)) {
			return DataTypes.STRING().nullable();
		}

		else if (typeInfo.equals(Types.BOOLEAN)) {
			return DataTypes.BOOLEAN().notNull();
		}

		else if (typeInfo instanceof PrimitiveArrayTypeInfo &&
				typeInfo.getTypeClass().equals(byte[].class)) {
			return DataTypes.BYTES().notNull();
		}

		else if (typeInfo.equals(Types.BIG_DEC)) {
			return DataTypes.DECIMAL(POST_1_9_DECIMAL_PRECISION, POST_1_9_DECIMAL_SCALE).nullable();
		}

		else if (typeInfo.equals(Types.BYTE)) {
			return DataTypes.TINYINT().notNull();
		}

		else if (typeInfo.equals(Types.SHORT)) {
			return DataTypes.SMALLINT().notNull();
		}

		else if (typeInfo.equals(Types.INT)) {
			return DataTypes.INT().notNull();
		}

		else if (typeInfo.equals(Types.LONG)) {
			return DataTypes.BIGINT().notNull();
		}

		else if (typeInfo.equals(Types.FLOAT)) {
			return DataTypes.FLOAT().notNull();
		}

		else if (typeInfo.equals(Types.DOUBLE)) {
			return DataTypes.DOUBLE().notNull();
		}

		else if (typeInfo.equals(Types.SQL_DATE)) {
			return DataTypes.DATE().nullable();
		}

		else if (typeInfo.equals(Types.SQL_TIME)) {
			return DataTypes.TIME(TIME_PRECISION).nullable();
		}

		// TODO drop this as soon as possible
		else if (typeInfo instanceof TimeIndicatorTypeInfo) {
			final TimeIndicatorTypeInfo timeIndicator = (TimeIndicatorTypeInfo) typeInfo;
			return new AtomicDataType(
				new TimestampType(
					false,
					timeIndicator.isEventTime() ? TimestampKind.ROWTIME : TimestampKind.PROCTIME,
					3));
		}

		else if (typeInfo.equals(Types.SQL_TIMESTAMP)) {
			return DataTypes.TIMESTAMP(POST_1_9_TIMESTAMP_PRECISION).nullable();
		}

		else if (typeInfo.equals(TimeIntervalTypeInfo.INTERVAL_MONTHS)) {
			return DataTypes.INTERVAL(DataTypes.MONTH()).notNull();
		}

		else if (typeInfo.equals(TimeIntervalTypeInfo.INTERVAL_MILLIS)) {
			return DataTypes.INTERVAL(DataTypes.SECOND(PRE_1_9_DAY_TIME_INTERVAL_PRECISION)).notNull();
		}

		else if (typeInfo instanceof PrimitiveArrayTypeInfo) {
			// never null
			final DataType elementType =
				convertToDataType(((PrimitiveArrayTypeInfo<?>) typeInfo).getComponentType(), withBridging);
			return DataTypes.ARRAY(elementType).notNull();
		}

		else if (typeInfo instanceof BasicArrayTypeInfo) {
			final DataType elementType =
				convertToDataType(((BasicArrayTypeInfo<?, ?>) typeInfo).getComponentInfo(), withBridging).nullable();
			return DataTypes.ARRAY(elementType).notNull();
		}

		else if (typeInfo instanceof ObjectArrayTypeInfo) {
			final DataType elementType =
				convertToDataType(((ObjectArrayTypeInfo<?, ?>) typeInfo).getComponentInfo(), withBridging).nullable();
			return DataTypes.ARRAY(elementType).notNull();
		}

		else if (typeInfo instanceof MultisetTypeInfo) {
			// null if the element supports it
			final DataType elementType = convertToDataType(((MultisetTypeInfo<?>) typeInfo).getElementTypeInfo(), withBridging);
			return DataTypes.MULTISET(elementType).notNull();
		}

		else if (typeInfo instanceof MapTypeInfo) {
			// null if the key supports it
			final DataType keyType =
				convertToDataType(((MapTypeInfo<?, ?>) typeInfo).getKeyTypeInfo(), withBridging);
			final DataType valueType =
				convertToDataType(((MapTypeInfo<?, ?>) typeInfo).getValueTypeInfo(), withBridging).nullable();
			return DataTypes.MAP(keyType, valueType).notNull();
		}

		else if (typeInfo instanceof RowTypeInfo) {
			return convertToRowType((CompositeType<?>) typeInfo, withBridging);
		}

		// used in formats
		else if (typeInfo.equals(Types.VOID)) {
			return DataTypes.NULL().nullable();
		}

		// types for back-and-forth conversion

		else if (typeInfo instanceof AnyTypeInformation) {
			return convertToDataTypeFromAnyTypeInfo((AnyTypeInformation<?>) typeInfo);
		}

		else if (typeInfo instanceof CompositeType) {
			return convertToStructuredType((CompositeType) typeInfo, withBridging);
		}

		// types that were introduced with Java 8 and later

		else if (typeInfo instanceof GenericTypeInfo) {
			return convertToDataTypeFromGenericTypeInfo((GenericTypeInfo<?>) typeInfo);
		}

		else if (typeInfo.equals(Types.INSTANT)) {
			return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(LocalZonedTimestampType.MAX_PRECISION).notNull();
		}

		// fallback (usually doesn't support null unless the composite type supports it)
		return DataTypes.ANY(typeInfo).notNull();
	}

	/**
	 * Classes with clearly defined semantics can be converted from generic type information.
	 */
	private static final Map<String, DataType> genericClassToDataType = new HashMap<>();
	static {
		genericClassToDataType.put(
			java.time.LocalDate.class.getName(),
			DataTypes.DATE());
		genericClassToDataType.put(
			java.time.LocalTime.class.getName(),
			DataTypes.TIME(TimeType.MAX_PRECISION));
		genericClassToDataType.put(
			java.time.LocalDateTime.class.getName(),
			DataTypes.TIMESTAMP(TimestampType.MAX_PRECISION));
		genericClassToDataType.put(
			java.time.ZonedDateTime.class.getName(),
			DataTypes.TIMESTAMP_WITH_TIME_ZONE(ZonedTimestampType.MAX_PRECISION));
		genericClassToDataType.put(
			java.time.OffsetDateTime.class.getName(),
			DataTypes.TIMESTAMP_WITH_TIME_ZONE(ZonedTimestampType.MAX_PRECISION));
		genericClassToDataType.put(
			java.time.Instant.class.getName(),
			DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(LocalZonedTimestampType.MAX_PRECISION));
		genericClassToDataType.put(
			java.time.Duration.class.getName(),
			DataTypes.INTERVAL(
				DataTypes.DAY(DayTimeIntervalType.MAX_DAY_PRECISION),
				DataTypes.SECOND(DayTimeIntervalType.MAX_FRACTIONAL_PRECISION)));
		genericClassToDataType.put(
			java.time.Period.class.getName(),
			DataTypes.INTERVAL(
				DataTypes.YEAR(YearMonthIntervalType.MAX_PRECISION),
				DataTypes.MONTH()));
	}

	private static DataType convertToDataTypeFromGenericTypeInfo(GenericTypeInfo<?> genericType) {
		// generic types support null
		return genericClassToDataType.getOrDefault(
				genericType.getTypeClass().getName(),
				DataTypes.ANY(genericType))
			.nullable();
	}

	private static DataType convertToRowType(CompositeType<?> compositeType, boolean withBridging) {
		return DataTypes.ROW(extractFields(compositeType, withBridging).toArray(new DataTypes.Field[0]))
			.notNull();
	}

	private static DataType convertToStructuredType(CompositeType<?> compositeType, boolean withBridging) {

		// for back-and-forth conversion
		if (compositeType instanceof StructuredTypeInformation) {
			return ((StructuredTypeInformation) compositeType).getDataType();
		}

		final TypeIdentifier typeIdentifier = new TypeIdentifier(compositeType.getTypeClass().getName());

		final List<DataTypes.Field> fields = extractFields(compositeType, withBridging);

		final List<StructuredAttribute> attributes = fields
			.stream()
			.map(f -> new StructuredAttribute(f.getName(), f.getDataType().getLogicalType()))
			.collect(Collectors.toList());

		final StructuredType.Builder builder = new StructuredType.Builder(typeIdentifier, attributes);
		// we are as restrictive as possible
		builder.setFinal(true);
		builder.setInstantiable(false);
		if (ReflectionUtils.isComparable(compositeType.getTypeClass())) {
			builder.setComparision(StructuredComparision.FULL);
		} else if (ReflectionUtils.hasEquals(compositeType.getTypeClass())) {
			builder.setComparision(StructuredComparision.EQUALS);
		}
		builder.setImplementationClass(compositeType.getTypeClass());

		// only POJOs support being null as well
		builder.setNullable(compositeType instanceof PojoTypeInfo);

		return new FieldsDataType(
			builder.build(),
			fields
				.stream()
				.collect(Collectors.toMap(DataTypes.Field::getName, DataTypes.Field::getDataType)));
	}

	private static List<DataTypes.Field> extractFields(CompositeType<?> compositeType, boolean withBridging) {
		// nullability of fields depends on the composite type
		final boolean containerSupportsNullability = compositeType instanceof RowTypeInfo ||
			compositeType instanceof PojoTypeInfo;

		final String[] fieldNames = compositeType.getFieldNames();
		return IntStream.range(0, compositeType.getArity())
			.mapToObj(i -> {
				final String fieldName = fieldNames[i];
				DataType fieldType = convertToDataType(compositeType.getTypeAt(i), withBridging);

				if (containerSupportsNullability) {
					fieldType = fieldType.nullable();
				}

				// special case for POJOs and case classes as they might store primitive types but
				// type information does not reflect that and returns a boxed type which is wrong
				if (compositeType instanceof TupleTypeInfo || compositeType instanceof PojoTypeInfo) {
					final Field field = ReflectionUtils.getDeclaredField(
						compositeType.getTypeClass(),
						fieldName);
					if (field != null && field.getType().isPrimitive()) {
						fieldType = fieldType.notNull();
						if (withBridging) {
							fieldType = fieldType.bridgedTo(field.getType());
						}
					}
				}

				return DataTypes.FIELD(
					fieldNames[i],
					fieldType);
			})
			.collect(Collectors.toList());
	}

	private static <T> DataType convertToDataTypeFromAnyTypeInfo(AnyTypeInformation<T> anyTypeInformation) {
		return DataTypes.ANY(
			anyTypeInformation.getAnyType().getOriginatingClass(),
			anyTypeInformation.getAnyType().getTypeSerializer());
	}

	// --------------------------------------------------------------------------------------------
	// From DataType to TypeInformation
	// --------------------------------------------------------------------------------------------

	/**
	 * Classes that are not covered by the type extractor.
	 */
	private static final Map<String, TypeInformation<?>> manualTypeInfo = new HashMap<>();
	static {
		putClassToTypeInfo(java.sql.Date.class, Types.SQL_DATE);
		putClassToTypeInfo(java.sql.Time.class, Types.SQL_TIME);
		putClassToTypeInfo(java.sql.Timestamp.class, Types.SQL_TIMESTAMP);
		putClassToTypeInfo(java.time.Instant.class, Types.INSTANT);
	}

	private static void putClassToTypeInfo(Class<?> clazz, TypeInformation<?> typeInfo) {
		manualTypeInfo.put(clazz.getName(), typeInfo);
	}

	private static TypeInformation<?> convertToTypeInfo(DataType dataType) {
		final LogicalType logicalType = dataType.getLogicalType();
		final Class<?> conversionClass = dataType.getConversionClass();

		switch (dataType.getLogicalType().getTypeRoot()) {

			case INTERVAL_DAY_TIME:
				final DayTimeIntervalType dayTimeIntervalType = (DayTimeIntervalType) logicalType;
				if (conversionClass.equals(Long.class)) {
					return TimeIntervalTypeInfo.INTERVAL_MILLIS;
				}
				return controlledTypeExtraction(conversionClass);

			case INTERVAL_YEAR_MONTH:
				final YearMonthIntervalType yearMonthIntervalType = (YearMonthIntervalType) logicalType;
				if (conversionClass.equals(Integer.class)) {
					return TimeIntervalTypeInfo.INTERVAL_MONTHS;
				}
				return controlledTypeExtraction(conversionClass);

			case TIMESTAMP_WITHOUT_TIME_ZONE:
				// TODO drop this as soon as possible
				final TimestampType timestampType = (TimestampType) logicalType;
				if (timestampType.getKind() == TimestampKind.ROWTIME) {
					return TimeIndicatorTypeInfo.ROWTIME_INDICATOR;
				} else if (timestampType.getKind() == TimestampKind.PROCTIME) {
					return TimeIndicatorTypeInfo.PROCTIME_INDICATOR;
				}
				return controlledTypeExtraction(conversionClass);

			case CHAR:
			case VARCHAR:
			case BOOLEAN:
			case BINARY:
			case VARBINARY:
			case DECIMAL:
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
			case FLOAT:
			case DOUBLE:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
			case ARRAY:
				final TypeInformation<?> typeInfo = manualTypeInfo.get(conversionClass.getName());
				if (typeInfo != null) {
					return typeInfo;
				}
				return controlledTypeExtraction(conversionClass);

			case MULTISET:
				final CollectionDataType collectionDataType = (CollectionDataType) dataType;
				return new MultisetTypeInfo<>(convertToTypeInfo(collectionDataType.getElementDataType()));

			case MAP:
				final KeyValueDataType keyValueDataType = (KeyValueDataType) dataType;
				return Types.MAP(
					convertToTypeInfo(keyValueDataType.getKeyDataType()),
					convertToTypeInfo(keyValueDataType.getValueDataType()));

			case ROW:
				if (conversionClass.equals(Row.class)) {
					return convertToRowTypeInfo((FieldsDataType) dataType);
				}
				return controlledTypeExtraction(conversionClass);

			case DISTINCT_TYPE:
				final DistinctType distinctType = (DistinctType) logicalType;
				// unwrap to source type
				return convertToTypeInfo(new AtomicDataType(distinctType.getSourceType(), conversionClass));

			case STRUCTURED_TYPE:
				return convertToCompositeTypeInfo((FieldsDataType) dataType);

			case NULL:
				return Types.GENERIC(Object.class);

			case ANY:
				if (dataType.getLogicalType() instanceof TypeInformationAnyType) {
					final TypeInformationAnyType anyType = (TypeInformationAnyType) logicalType;
					return anyType.getTypeInformation();
				} else {
					final AnyType<?> anyType = (AnyType<?>) logicalType;
					return new AnyTypeInformation<>(anyType);
				}

			default:
				throw new TableException("Unsupported data type: " + dataType);
		}
	}

	private static TypeInformation<?> controlledTypeExtraction(Class<?> clazz) {
		try {
			return TypeExtractor.createTypeInfo(clazz);
		} catch (Throwable t) {
			throw new TableException("Unexpected type extraction exception. This is a bug.", t);
		}
	}

	private static TypeInformation<?> convertToRowTypeInfo(FieldsDataType fieldsDataType) {
		final RowType rowType = (RowType) fieldsDataType.getLogicalType();

		final String[] fieldNames = rowType.getFields()
			.stream()
			.map(RowType.RowField::getName)
			.toArray(String[]::new);

		final TypeInformation<?>[] fieldTypes = Stream.of(fieldNames)
			.map(name -> fieldsDataType.getFieldDataTypes().get(name))
			.map(TypeInfoDataTypeConverter::convertToTypeInfo)
			.toArray(TypeInformation[]::new);

		return Types.ROW_NAMED(fieldNames, fieldTypes);
	}

	@SuppressWarnings("unchecked")
	private static TypeInformation<?> convertToCompositeTypeInfo(FieldsDataType fieldsDataType) {
		final StructuredType structuredType = (StructuredType) fieldsDataType.getLogicalType();
		final Class<?> conversionClass = fieldsDataType.getConversionClass();

		final String[] fieldNames = structuredType.getAttributes()
				.stream()
				.map(StructuredType.StructuredAttribute::getName)
				.toArray(String[]::new);

		final TypeInformation<?>[] fieldTypes = Stream.of(fieldNames)
			.map(name -> fieldsDataType.getFieldDataTypes().get(name))
			.map(TypeInfoDataTypeConverter::convertToTypeInfo)
			.toArray(TypeInformation[]::new);

		if (conversionClass.equals(Row.class)) {
			return Types.ROW_NAMED(fieldNames, fieldTypes);
		} else if (conversionClass.getName().equals("org.apache.flink.table.dataformat.BaseRow")) {
			return controlledTypeExtraction(fieldsDataType.getConversionClass());
		} else {
			return new StructuredTypeInformation(
				conversionClass,
				fieldsDataType,
				fieldTypes);
		}
	}
}
