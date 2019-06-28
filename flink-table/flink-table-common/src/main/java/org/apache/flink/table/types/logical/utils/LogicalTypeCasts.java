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

package org.apache.flink.table.types.logical.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.DAY;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.DAY_TO_HOUR;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.DAY_TO_MINUTE;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.HOUR;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.HOUR_TO_MINUTE;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.HOUR_TO_SECOND;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.MINUTE;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.MINUTE_TO_SECOND;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.SECOND;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.APPROXIMATE_NUMERIC;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.BINARY_STRING;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.CHARACTER_STRING;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.CONSTRUCTED;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.DATETIME;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.EXACT_NUMERIC;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.INTERVAL;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.NUMERIC;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.PREDEFINED;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.TIME;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.TIMESTAMP;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.ANY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.ARRAY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BOOLEAN;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.CHAR;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DATE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DISTINCT_TYPE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DOUBLE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.FLOAT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_DAY_TIME;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_YEAR_MONTH;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.MULTISET;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.NULL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.ROW;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.SMALLINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.STRUCTURED_TYPE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.SYMBOL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TINYINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARBINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.VARCHAR;
import static org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution.MONTH;
import static org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution.YEAR;
import static org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getLength;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasFamily;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isSingleFieldInterval;

/**
 * Utilities for casting {@link LogicalType}.
 *
 * <p>This class aims to be compatible with the SQL standard cast specification.
 *
 * <p>Conversions that are defined by the {@link LogicalType} (e.g. interpreting a {@link DateType}
 * as integer value) are not considered here. They are an internal bridging feature that is not
 * standard compliant. If at all, {@code CONVERT} methods should make such conversions available.
 */
@Internal
public final class LogicalTypeCasts {

	private static final Map<LogicalTypeRoot, Set<LogicalTypeRoot>> implicitCastingRules;

	private static final Map<LogicalTypeRoot, Set<LogicalTypeRoot>> explicitCastingRules;

	static {
		implicitCastingRules = new HashMap<>();
		explicitCastingRules = new HashMap<>();

		// identity casts

		for (LogicalTypeRoot typeRoot : allTypes()) {
			castTo(typeRoot)
				.implicitFrom(typeRoot)
				.build();
		}

		// cast specification

		castTo(CHAR)
			.implicitFrom(CHAR)
			.explicitFromFamily(PREDEFINED)
			.explicitNotFromFamily(BINARY_STRING)
			.build();

		castTo(VARCHAR)
			.implicitFromFamily(CHARACTER_STRING)
			.explicitFromFamily(PREDEFINED)
			.explicitNotFromFamily(BINARY_STRING)
			.build();

		castTo(BOOLEAN)
			.implicitFrom(BOOLEAN)
			.explicitFromFamily(CHARACTER_STRING)
			.build();

		castTo(BINARY)
			.implicitFromFamily(BINARY_STRING)
			.build();

		castTo(VARBINARY)
			.implicitFromFamily(BINARY_STRING)
			.build();

		castTo(DECIMAL)
			.implicitFromFamily(NUMERIC)
			.explicitFromFamily(CHARACTER_STRING)
			.build();

		castTo(TINYINT)
			.implicitFrom(TINYINT)
			.explicitFromFamily(NUMERIC, CHARACTER_STRING)
			.build();

		castTo(SMALLINT)
			.implicitFrom(TINYINT, SMALLINT)
			.explicitFromFamily(NUMERIC, CHARACTER_STRING)
			.build();

		castTo(INTEGER)
			.implicitFrom(TINYINT, SMALLINT, INTEGER)
			.explicitFromFamily(NUMERIC, CHARACTER_STRING)
			.build();

		castTo(BIGINT)
			.implicitFrom(TINYINT, SMALLINT, INTEGER, BIGINT)
			.explicitFromFamily(NUMERIC, CHARACTER_STRING)
			.build();

		castTo(FLOAT)
			.implicitFrom(TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DECIMAL)
			.explicitFromFamily(NUMERIC, CHARACTER_STRING)
			.build();

		castTo(DOUBLE)
			.implicitFrom(TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL)
			.explicitFromFamily(CHARACTER_STRING)
			.build();

		castTo(DATE)
			.implicitFrom(DATE, TIMESTAMP_WITHOUT_TIME_ZONE)
			.explicitFromFamily(TIMESTAMP, CHARACTER_STRING)
			.build();

		castTo(TIME_WITHOUT_TIME_ZONE)
			.implicitFrom(TIME_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE)
			.explicitFromFamily(TIME, TIMESTAMP, CHARACTER_STRING)
			.build();

		castTo(TIMESTAMP_WITHOUT_TIME_ZONE)
			.implicitFrom(TIMESTAMP_WITHOUT_TIME_ZONE)
			.explicitFromFamily(DATETIME, CHARACTER_STRING)
			.build();

		castTo(TIMESTAMP_WITH_TIME_ZONE)
			.implicitFrom(TIMESTAMP_WITH_TIME_ZONE)
			.explicitFromFamily(DATETIME, CHARACTER_STRING)
			.build();

		castTo(TIMESTAMP_WITH_LOCAL_TIME_ZONE)
			.implicitFrom(TIMESTAMP_WITH_LOCAL_TIME_ZONE)
			.explicitFromFamily(DATETIME, CHARACTER_STRING)
			.build();

		castTo(INTERVAL_YEAR_MONTH)
			.implicitFrom(INTERVAL_YEAR_MONTH)
			.explicitFromFamily(EXACT_NUMERIC, CHARACTER_STRING)
			.build();

		castTo(INTERVAL_DAY_TIME)
			.implicitFrom(INTERVAL_DAY_TIME)
			.explicitFromFamily(EXACT_NUMERIC, CHARACTER_STRING)
			.build();
	}

	public static boolean supportsImplicitCast(LogicalType sourceType, LogicalType targetType) {
		return supportsCasting(sourceType, targetType, false);
	}

	public static boolean supportsExplicitCast(LogicalType sourceType, LogicalType targetType) {
		return supportsCasting(sourceType, targetType, true);
	}

	/**
	 * Returns the most common type of a set of types. It determines a type to which all given types
	 * can be casted.
	 *
	 * <p>For example: {@code [INT, DECIMAL(12,12)]} would lead to {@code DECIMAL(12,12)}
	 *
	 * <p>This method is heavily inspired by Calcite's {@code SqlTypeFactoryImpl} but adapted to
	 * Flink's type system.
	 */
	public static Optional<LogicalType> findCommonType(List<LogicalType> types) {
		Preconditions.checkArgument(types.size() > 0);

		// collect statistics first
		boolean hasAnyType = false;
		boolean hasNullType = false;
		boolean hasNullableTypes = false;
		for (LogicalType type : types) {
			final LogicalTypeRoot typeRoot = type.getTypeRoot();
			if (typeRoot == ANY) {
				hasAnyType = true;
			} else if (typeRoot == NULL) {
				hasNullType = true;
			}
			if (type.isNullable()) {
				hasNullableTypes = true;
			}
		}

		final List<LogicalType> normalizedTypes = types.stream()
			.map(t -> t.copy(true))
			.collect(Collectors.toList());

		LogicalType foundType = findLeastRestrictive(normalizedTypes, hasAnyType, hasNullType);
		if (foundType == null) {
			foundType = findLeastRestrictiveByCast(normalizedTypes);
		}

		if (foundType != null) {
			final LogicalType typeWithNullability = foundType.copy(hasNullableTypes);
			return Optional.of(typeWithNullability);
		}
		return Optional.empty();
	}

	private static @Nullable LogicalType findLeastRestrictivePattern(LogicalType resultType, LogicalType type) {
		if (hasFamily(resultType, DATETIME) && hasFamily(type, INTERVAL)) {
			return resultType;
		} else if (hasFamily(resultType, INTERVAL) && hasFamily(type, DATETIME)) {
			return type;
		} else if ((hasFamily(resultType, TIMESTAMP) || hasRoot(resultType, DATE)) && hasFamily(type, EXACT_NUMERIC)) {
			return resultType;
		} else if (hasFamily(resultType, EXACT_NUMERIC) && (hasFamily(type, TIMESTAMP) || hasRoot(type, DATE))) {
			return type;
		}
		return null;
	}

	@SuppressWarnings("ConstantConditions")
	private static @Nullable LogicalType findLeastRestrictive(
			List<LogicalType> normalizedTypes,
			boolean hasAnyType,
			boolean hasNullType) {

		// all ANY types must be equal
		if (hasAnyType) {
			return findSameType(normalizedTypes);
		}

		LogicalType resultType = null;

		for (LogicalType type : normalizedTypes) {
			final LogicalTypeRoot typeRoot = type.getTypeRoot();

			// NULL does not affect the result of this loop
			if (typeRoot == NULL) {
				continue;
			}

			// if result type is still null, consider the current type as a potential
			// result type candidate
			if (resultType == null) {
				resultType = type;
			}

			// find special patterns
			final LogicalType patternType = findLeastRestrictivePattern(resultType, type);
			if (patternType != null) {
				resultType = patternType;
				continue;
			}

			// for types of family CONSTRUCTED
			if (typeRoot == ARRAY) {
				return findLeastRestrictiveArrayType(normalizedTypes);
			} else if (typeRoot == MULTISET) {
				return findLeastRestrictiveMultisetType(normalizedTypes);
			} else if (typeRoot == ROW) {
				return findLeastRestrictiveRowType(normalizedTypes);
			}

			// exit if two completely different types are compared (e.g. ROW and INT)
			// this simplifies the following lines as we compare same interval families for example
			if (!areSimilarTypes(resultType, type)) {
				return null;
			}

			// for types of family CHARACTER_STRING or BINARY_STRING
			if (hasFamily(type, CHARACTER_STRING) || hasFamily(type, BINARY_STRING)) {
				final int length = getMaxLength(resultType, type);

				if (hasRoot(resultType, VARCHAR) || hasRoot(resultType, VARBINARY)) {
					// for variable length type we are done here
					resultType = createStringType(resultType.getTypeRoot(), length);
				} else {
					// for mixed fixed/variable or fixed/fixed lengths
					resultType = createStringType(typeRoot, length);
				}
			}
			// for EXACT_NUMERIC types
			else if (hasFamily(type, EXACT_NUMERIC)) {
				if (hasFamily(resultType, EXACT_NUMERIC)) {
					resultType = getLeastRestrictiveExactNumericType(resultType, type);
				} else if (hasFamily(resultType, APPROXIMATE_NUMERIC)) {
					// the result is already approximate
					if (typeRoot == DECIMAL) {
						// in case of DECIMAL we enforce DOUBLE
						resultType = new DoubleType();
					}
				} else {
					return null;
				}
			}
			// for APPROXIMATE_NUMERIC types
			else if (hasFamily(type, APPROXIMATE_NUMERIC)) {
				if (hasFamily(type, APPROXIMATE_NUMERIC)) {
					resultType = getLeastRestrictiveApproximateNumericType(resultType, type);
				} else if (hasFamily(resultType, EXACT_NUMERIC)) {
					// the result was exact so far
					if (typeRoot == DECIMAL) {
						// in case of DECIMAL we enforce DOUBLE
						resultType = new DoubleType();
					} else {
						// enforce an approximate result
						resultType = type;
					}
				} else {
					return null;
				}
			}
			// for TIME
			else if (hasFamily(type, TIME)) {
				if (hasFamily(resultType, TIME)) {
					resultType = new TimeType(
						getLeastRestrictivePrecision(resultType, type));
				} else {
					return null;
				}
			}
			// for TIMESTAMP
			else if (hasFamily(type, TIMESTAMP)) {
				if (hasFamily(resultType, TIMESTAMP)) {
					resultType = getLeastRestrictiveTimestampType(resultType, type);
				} else {
					return null;
				}
			}
			// for day-time intervals
			else if (typeRoot == INTERVAL_DAY_TIME) {
				resultType = getLeastRestrictiveDayTimeIntervalType(
					(DayTimeIntervalType) resultType,
					(DayTimeIntervalType) type);
			}
			// for year-month intervals
			else if (typeRoot == INTERVAL_YEAR_MONTH) {
				resultType = getLeastRestrictiveYearMonthIntervalType(
					(YearMonthIntervalType) resultType,
					(YearMonthIntervalType) type);
			}
			// other types are handled by leastRestrictiveByCast
			else {
				return null;
			}
		}

		// NULL type only
		if (resultType == null && hasNullType) {
			return new NullType();
		}

		return resultType;
	}

	private static LogicalType getLeastRestrictiveTimestampType(LogicalType resultType, LogicalType type) {
		// same types
		if (type.equals(resultType)) {
			return resultType;
		}

		final LogicalTypeRoot resultTypeRoot = resultType.getTypeRoot();
		final LogicalTypeRoot typeRoot = type.getTypeRoot();
		final int precision = getLeastRestrictivePrecision(resultType, type);

		// same type roots
		if (typeRoot == resultTypeRoot) {
			return createTimestampType(resultTypeRoot, precision);
		}

		// generalize to zoned type
		if (typeRoot == TIMESTAMP_WITH_TIME_ZONE ||
				resultTypeRoot == TIMESTAMP_WITH_TIME_ZONE) {
			return createTimestampType(TIMESTAMP_WITH_TIME_ZONE, precision);
		} else if (typeRoot == TIMESTAMP_WITH_LOCAL_TIME_ZONE ||
				resultTypeRoot == TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
			return createTimestampType(TIMESTAMP_WITH_LOCAL_TIME_ZONE, precision);
		}
		return createTimestampType(TIMESTAMP_WITHOUT_TIME_ZONE, precision);
	}

	private static LogicalType createTimestampType(LogicalTypeRoot typeRoot, int precision) {
		switch (typeRoot) {
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return new TimestampType(precision);
			case TIMESTAMP_WITH_TIME_ZONE:
				return new ZonedTimestampType(precision);
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return new LocalZonedTimestampType(precision);
			default:
				throw new IllegalArgumentException();
		}
	}

	private static int getLeastRestrictivePrecision(
			LogicalType resultType,
			LogicalType type) {
		final int p1 = getPrecision(resultType);
		final int p2 = getPrecision(type);

		return Math.max(p1, p2);
	}

	private static LogicalType getLeastRestrictiveDayTimeIntervalType(DayTimeIntervalType resultType, DayTimeIntervalType type) {
		final int maxDayPrecision = Math.max(resultType.getDayPrecision(), type.getDayPrecision());
		final int maxFractionalPrecision = Math.max(resultType.getFractionalPrecision(), type.getFractionalPrecision());
		return new DayTimeIntervalType(
			combineResolutions(
				DayTimeResolution.values(),
				DAY_TIME_RES_TO_BOUNDARIES,
				DAY_TIME_BOUNDARIES_TO_RES,
				resultType.getResolution(),
				type.getResolution()),
			maxDayPrecision,
			maxFractionalPrecision);
	}

	private static LogicalType getLeastRestrictiveYearMonthIntervalType(YearMonthIntervalType resultType, YearMonthIntervalType type) {
		final int maxYearPrecision = Math.max(resultType.getYearPrecision(), type.getYearPrecision());
		return new YearMonthIntervalType(
			combineResolutions(
				YearMonthResolution.values(),
				YEAR_MONTH_RES_TO_BOUNDARIES,
				YEAR_MONTH_BOUNDARIES_TO_RES,
				resultType.getResolution(),
				type.getResolution()),
			maxYearPrecision);
	}

	private static final Map<DayTimeResolution, List<DayTimeResolution>> DAY_TIME_RES_TO_BOUNDARIES = new HashMap<>();
	private static final Map<List<DayTimeResolution>, DayTimeResolution> DAY_TIME_BOUNDARIES_TO_RES = new HashMap<>();
	static {
		addDayTimeMapping(DAY, DAY);
		addDayTimeMapping(DAY_TO_HOUR, DAY, HOUR);
		addDayTimeMapping(DAY_TO_MINUTE, DAY, MINUTE);
		addDayTimeMapping(DAY_TO_SECOND, DAY, SECOND);
		addDayTimeMapping(HOUR, HOUR);
		addDayTimeMapping(HOUR_TO_MINUTE, HOUR, MINUTE);
		addDayTimeMapping(HOUR_TO_SECOND, HOUR, SECOND);
		addDayTimeMapping(MINUTE, MINUTE);
		addDayTimeMapping(MINUTE_TO_SECOND, MINUTE, SECOND);
		addDayTimeMapping(SECOND, SECOND);
	}

	private static void addDayTimeMapping(DayTimeResolution to, DayTimeResolution... boundaries) {
		final List<DayTimeResolution> boundariesList = Arrays.asList(boundaries);
		DAY_TIME_RES_TO_BOUNDARIES.put(to, boundariesList);
		DAY_TIME_BOUNDARIES_TO_RES.put(boundariesList, to);
	}

	private static final Map<YearMonthResolution, List<YearMonthResolution>> YEAR_MONTH_RES_TO_BOUNDARIES = new HashMap<>();
	private static final Map<List<YearMonthResolution>, YearMonthResolution> YEAR_MONTH_BOUNDARIES_TO_RES = new HashMap<>();
	static {
		addYearMonthMapping(YEAR, YEAR);
		addYearMonthMapping(MONTH, MONTH);
		addYearMonthMapping(YEAR_TO_MONTH, YEAR, MONTH);
	}

	private static void addYearMonthMapping(YearMonthResolution to, YearMonthResolution... boundaries) {
		final List<YearMonthResolution> boundariesList = Arrays.asList(boundaries);
		YEAR_MONTH_RES_TO_BOUNDARIES.put(to, boundariesList);
		YEAR_MONTH_BOUNDARIES_TO_RES.put(boundariesList, to);
	}

	private static <T extends Enum<T>> T combineResolutions(
			T[] res,
			Map<T, List<T>> resToBoundaries,
			Map<List<T>, T> boundariesToRes,
			T left,
			T right) {
		final List<T> leftBoundaries = resToBoundaries.get(left);
		final T leftStart = leftBoundaries.get(0);
		final T leftEnd = leftBoundaries.get(leftBoundaries.size() - 1);

		final List<T> rightBoundaries = resToBoundaries.get(right);
		final T rightStart = rightBoundaries.get(0);
		final T rightEnd = rightBoundaries.get(rightBoundaries.size() - 1);

		final T combinedStart = res[Math.min(leftStart.ordinal(), rightStart.ordinal())];
		final T combinedEnd = res[Math.max(leftEnd.ordinal(), rightEnd.ordinal())];

		if (combinedStart == combinedEnd) {
			return boundariesToRes.get(Collections.singletonList(combinedStart));
		}
		return boundariesToRes.get(Arrays.asList(combinedStart, combinedEnd));
	}

	private static LogicalType getLeastRestrictiveApproximateNumericType(LogicalType resultType, LogicalType type) {
		if (hasRoot(resultType, DOUBLE) || hasRoot(type, DOUBLE)) {
			return new DoubleType();
		}
		return resultType;
	}

	private static LogicalType getLeastRestrictiveExactNumericType(LogicalType resultType, LogicalType type) {
		// same EXACT_NUMERIC types
		if (type.equals(resultType)) {
			return resultType;
		}

		final LogicalTypeRoot resultTypeRoot = resultType.getTypeRoot();
		final LogicalTypeRoot typeRoot = type.getTypeRoot();

		// no DECIMAL types involved
		if (resultTypeRoot != DECIMAL && typeRoot != DECIMAL) {
			// type root contains order of precision
			if (getPrecision(type) > getPrecision(resultType)) {
				return type;
			}
			return resultType;
		}

		// determine DECIMAL with precision (p), scale (s) and number of whole digits (d):
		// d = max(p1 - s1, p2 - s2)
		// s <= max(s1, s2)
		// p = s + d
		final int p1 = getPrecision(resultType);
		final int p2 = getPrecision(type);
		final int s1 = getScale(resultType);
		final int s2 = getScale(type);
		final int maxPrecision = DecimalType.MAX_PRECISION;

		int d = Math.max(p1 - s1, p2 - s2);
		d = Math.min(d, maxPrecision);

		int s = Math.max(s1, s2);
		s = Math.min(s, maxPrecision - d);

		final int p = d + s;

		return new DecimalType(p, s);
	}

	private static boolean areSimilarTypes(LogicalType left, LogicalType right) {
		// two types are similar iff they can be the operands of an SQL equality predicate

		// similarity based on families
		if (hasFamily(left, CHARACTER_STRING) && hasFamily(right, CHARACTER_STRING)) {
			return true;
		} else if (hasFamily(left, BINARY_STRING) && hasFamily(right, BINARY_STRING)) {
			return true;
		} else if (hasFamily(left, NUMERIC) && hasFamily(right, NUMERIC)) {
			return true;
		} else if (hasFamily(left, TIME) && hasFamily(right, TIME)) {
			return true;
		} else if (hasFamily(left, TIMESTAMP) && hasFamily(right, TIMESTAMP)) {
			return true;
		}
		// similarity based on root
		return left.getTypeRoot() == right.getTypeRoot();
	}

	private static LogicalType createStringType(LogicalTypeRoot typeRoot, int length) {
		switch (typeRoot) {
			case CHAR:
				return new CharType(length);
			case VARCHAR:
				return new VarCharType(length);
			case BINARY:
				return new BinaryType(length);
			case VARBINARY:
				return new VarBinaryType(length);
			default:
				throw new IllegalArgumentException();
		}
	}

	private static @Nullable LogicalType findLeastRestrictiveMultisetType(List<LogicalType> normalizedTypes) {
		final List<LogicalType> children = findLeastRestrictiveChildren(normalizedTypes);
		if (children == null) {
			return null;
		}
		return new MultisetType(children.get(0));
	}

	private static @Nullable LogicalType findLeastRestrictiveArrayType(List<LogicalType> normalizedTypes) {
		final List<LogicalType> children = findLeastRestrictiveChildren(normalizedTypes);
		if (children == null) {
			return null;
		}
		return new ArrayType(children.get(0));
	}

	private static @Nullable LogicalType findLeastRestrictiveRowType(List<LogicalType> normalizedTypes) {
		final List<LogicalType> children = findLeastRestrictiveChildren(normalizedTypes);
		if (children == null) {
			return null;
		}
		final RowType firstType = (RowType) normalizedTypes.get(0);
		final List<RowType.RowField> newFields = IntStream.range(0, children.size())
			.mapToObj(pos -> {
				final LogicalType newType = children.get(pos);
				final RowType.RowField originalField = firstType.getFields().get(pos);
				if (originalField.getDescription().isPresent()) {
					return new RowType.RowField(
						originalField.getName(),
						newType,
						originalField.getDescription().get());
				} else {
					return new RowType.RowField(
						originalField.getName(),
						newType);
				}
			})
			.collect(Collectors.toList());
		return new RowType(newFields);
	}

	private static @Nullable List<LogicalType> findLeastRestrictiveChildren(List<LogicalType> normalizedTypes) {
		final LogicalType firstType = normalizedTypes.get(0);
		final LogicalTypeRoot typeRoot = firstType.getTypeRoot();
		final int numberOfChildren = firstType.getChildren().size();

		for (LogicalType type : normalizedTypes) {
			// all types must have the same root
			if (type.getTypeRoot() != typeRoot) {
				return null;
			}
			// all types must have the same number of children
			if (type.getChildren().size() != numberOfChildren) {
				return null;
			}
		}

		// recursively compute column-wise least restrictive
		final List<LogicalType> resultChildren = new ArrayList<>(numberOfChildren);
		for (int i = 0; i < numberOfChildren; i++) {
			final int childPos = i;
			final Optional<LogicalType> childType = findCommonType(new AbstractList<LogicalType>() {
				@Override
				public LogicalType get(int index) {
					return normalizedTypes.get(index).getChildren().get(childPos);
				}

				@Override
				public int size() {
					return normalizedTypes.size();
				}
			});
			if (!childType.isPresent()) {
				return null;
			}
			resultChildren.add(childType.get());
		}
		// no child should be empty at this point
		return resultChildren;
	}

	private static @Nullable LogicalType findSameType(List<LogicalType> normalizedTypes) {
		final LogicalType firstType = normalizedTypes.get(0);
		for (LogicalType type : normalizedTypes) {
			if (!type.equals(firstType)) {
				return null;
			}
		}
		return firstType;
	}

	private static @Nullable LogicalType findLeastRestrictiveByCast(List<LogicalType> normalizedTypes) {
		LogicalType resultType = normalizedTypes.get(0);

		for (LogicalType type : normalizedTypes) {
			final LogicalTypeRoot typeRoot = type.getTypeRoot();

			// NULL does not affect the result of this loop
			if (typeRoot == NULL) {
				continue;
			}

			if (supportsImplicitCast(resultType, type)) {
				resultType = type;
			} else {
				if (!supportsImplicitCast(type, resultType)) {
					return null;
				}
			}
		}

		return resultType;
	}

	private static int getMaxLength(LogicalType left, LogicalType right) {
		return Math.max(getLength(left), getLength(right));
	}

	// --------------------------------------------------------------------------------------------

	private static boolean supportsCasting(
			LogicalType sourceType,
			LogicalType targetType,
			boolean allowExplicit) {
		if (sourceType.isNullable() && !targetType.isNullable()) {
			return false;
		}
		// ignore nullability during compare
		if (sourceType.copy(true).equals(targetType.copy(true))) {
			return true;
		}

		final LogicalTypeRoot sourceRoot = sourceType.getTypeRoot();
		final LogicalTypeRoot targetRoot = targetType.getTypeRoot();

		if (hasFamily(sourceType, INTERVAL) && hasFamily(targetType, EXACT_NUMERIC)) {
			// cast between interval and exact numeric is only supported if interval has a single field
			return isSingleFieldInterval(sourceType);
		} else if (hasFamily(sourceType, EXACT_NUMERIC) && hasFamily(targetType, INTERVAL)) {
			// cast between interval and exact numeric is only supported if interval has a single field
			return isSingleFieldInterval(targetType);
		} else if (hasFamily(sourceType, CONSTRUCTED) || hasFamily(targetType, CONSTRUCTED)) {
			return supportsConstructedCasting(sourceType, targetType, allowExplicit);
		} else if (sourceRoot == DISTINCT_TYPE && targetRoot == DISTINCT_TYPE) {
			// the two distinct types are not equal (from initial invariant), casting is not possible
			return false;
		} else if (sourceRoot == DISTINCT_TYPE) {
			return supportsCasting(((DistinctType) sourceType).getSourceType(), targetType, allowExplicit);
		} else if (targetRoot == DISTINCT_TYPE) {
			return supportsCasting(sourceType, ((DistinctType) targetType).getSourceType(), allowExplicit);
		} else if (sourceRoot == STRUCTURED_TYPE || targetRoot == STRUCTURED_TYPE) {
			// TODO structured types are not supported yet
			return false;
		} else if (sourceRoot == NULL) {
			// null can be cast to an arbitrary type
			return true;
		} else if (sourceRoot == ANY || targetRoot == ANY) {
			// the two any types are not equal (from initial invariant), casting is not possible
			return false;
		} else if (sourceRoot == SYMBOL || targetRoot == SYMBOL) {
			// the two symbol types are not equal (from initial invariant), casting is not possible
			return false;
		}

		if (implicitCastingRules.get(targetRoot).contains(sourceRoot)) {
			return true;
		}
		if (allowExplicit) {
			return explicitCastingRules.get(targetRoot).contains(sourceRoot);
		}
		return false;
	}

	private static boolean supportsConstructedCasting(
			LogicalType sourceType,
			LogicalType targetType,
			boolean allowExplicit) {
		final LogicalTypeRoot sourceRoot = sourceType.getTypeRoot();
		final LogicalTypeRoot targetRoot = targetType.getTypeRoot();
		// all constructed types can only be casted within the same type root
		if (sourceRoot == targetRoot) {
			final List<LogicalType> sourceChildren = sourceType.getChildren();
			final List<LogicalType> targetChildren = targetType.getChildren();
			if (sourceChildren.size() != targetChildren.size()) {
				return false;
			}
			for (int i = 0; i < sourceChildren.size(); i++) {
				if (!supportsCasting(sourceChildren.get(i), targetChildren.get(i), allowExplicit)) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	private static CastingRuleBuilder castTo(LogicalTypeRoot sourceType) {
		return new CastingRuleBuilder(sourceType);
	}

	private static LogicalTypeRoot[] allTypes() {
		return LogicalTypeRoot.values();
	}

	private static class CastingRuleBuilder {

		private final LogicalTypeRoot sourceType;
		private Set<LogicalTypeRoot> implicitTargetTypes = new HashSet<>();
		private Set<LogicalTypeRoot> explicitTargetTypes = new HashSet<>();

		CastingRuleBuilder(LogicalTypeRoot sourceType) {
			this.sourceType = sourceType;
		}

		CastingRuleBuilder implicitFrom(LogicalTypeRoot... targetTypes) {
			this.implicitTargetTypes.addAll(Arrays.asList(targetTypes));
			return this;
		}

		CastingRuleBuilder implicitFromFamily(LogicalTypeFamily... targetFamilies) {
			for (LogicalTypeFamily family : targetFamilies) {
				for (LogicalTypeRoot root : LogicalTypeRoot.values()) {
					if (root.getFamilies().contains(family)) {
						this.implicitTargetTypes.add(root);
					}
				}
			}
			return this;
		}

		CastingRuleBuilder explicitFrom(LogicalTypeRoot... targetTypes) {
			this.explicitTargetTypes.addAll(Arrays.asList(targetTypes));
			return this;
		}

		CastingRuleBuilder explicitFromFamily(LogicalTypeFamily... targetFamilies) {
			for (LogicalTypeFamily family : targetFamilies) {
				for (LogicalTypeRoot root : LogicalTypeRoot.values()) {
					if (root.getFamilies().contains(family)) {
						this.explicitTargetTypes.add(root);
					}
				}
			}
			return this;
		}

		CastingRuleBuilder explicitNotFromFamily(LogicalTypeFamily... targetFamilies) {
			for (LogicalTypeFamily family : targetFamilies) {
				for (LogicalTypeRoot root : LogicalTypeRoot.values()) {
					if (root.getFamilies().contains(family)) {
						this.explicitTargetTypes.remove(root);
					}
				}
			}
			return this;
		}

		void build() {
			implicitCastingRules.put(sourceType, implicitTargetTypes);
			explicitCastingRules.put(sourceType, explicitTargetTypes);
		}
	}

	private LogicalTypeCasts() {
		// no instantiation
	}
}
