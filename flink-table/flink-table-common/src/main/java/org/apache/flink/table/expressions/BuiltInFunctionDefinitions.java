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

package org.apache.flink.table.expressions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.table.functions.FunctionDefinition.FunctionKind.AGGREGATE_FUNCTION;
import static org.apache.flink.table.functions.FunctionDefinition.FunctionKind.OTHER_FUNCTION;
import static org.apache.flink.table.functions.FunctionDefinition.FunctionKind.SCALAR_FUNCTION;

/**
 * Dictionary of function definitions for all built-in functions.
 */
@PublicEvolving
public final class BuiltInFunctionDefinitions {

	public static class TempFunctionDefinition implements FunctionDefinition {

		public TempFunctionDefinition(String name, FunctionKind kind) {

		}

		@Override
		public String getName() {
			return null;
		}

		@Override
		public FunctionKind getKind() {
			return null;
		}

		@Override
		public TypeInference getTypeInference() {
			return null;
		}
	}

	// logic functions
	public static final FunctionDefinition AND =
		new TempFunctionDefinition("and", SCALAR_FUNCTION);
	public static final FunctionDefinition OR =
		new TempFunctionDefinition("or", SCALAR_FUNCTION);
	public static final FunctionDefinition NOT =
		new TempFunctionDefinition("not", SCALAR_FUNCTION);
	public static final FunctionDefinition IF =
		new TempFunctionDefinition("ifThenElse", SCALAR_FUNCTION);

	// comparison functions
	public static final FunctionDefinition EQUALS =
		new TempFunctionDefinition("equals", SCALAR_FUNCTION);
	public static final FunctionDefinition GREATER_THAN =
		new TempFunctionDefinition("greaterThan", SCALAR_FUNCTION);
	public static final FunctionDefinition GREATER_THAN_OR_EQUAL =
		new TempFunctionDefinition("greaterThanOrEqual", SCALAR_FUNCTION);
	public static final FunctionDefinition LESS_THAN =
		new TempFunctionDefinition("lessThan", SCALAR_FUNCTION);
	public static final FunctionDefinition LESS_THAN_OR_EQUAL =
		new TempFunctionDefinition("lessThanOrEqual", SCALAR_FUNCTION);
	public static final FunctionDefinition NOT_EQUALS =
		new TempFunctionDefinition("notEquals", SCALAR_FUNCTION);
	public static final FunctionDefinition IS_NULL =
		new TempFunctionDefinition("isNull", SCALAR_FUNCTION);
	public static final FunctionDefinition IS_NOT_NULL =
		new TempFunctionDefinition("isNotNull", SCALAR_FUNCTION);
	public static final FunctionDefinition IS_TRUE =
		new TempFunctionDefinition("isTrue", SCALAR_FUNCTION);
	public static final FunctionDefinition IS_FALSE =
		new TempFunctionDefinition("isFalse", SCALAR_FUNCTION);
	public static final FunctionDefinition IS_NOT_TRUE =
		new TempFunctionDefinition("isNotTrue", SCALAR_FUNCTION);
	public static final FunctionDefinition IS_NOT_FALSE =
		new TempFunctionDefinition("isNotFalse", SCALAR_FUNCTION);
	public static final FunctionDefinition BETWEEN =
		new TempFunctionDefinition("between", SCALAR_FUNCTION);
	public static final FunctionDefinition NOT_BETWEEN =
		new TempFunctionDefinition("notBetween", SCALAR_FUNCTION);

	// aggregate functions
	public static final FunctionDefinition AVG =
		new TempFunctionDefinition("avg", AGGREGATE_FUNCTION);
	public static final FunctionDefinition COUNT =
		new TempFunctionDefinition("count", AGGREGATE_FUNCTION);
	public static final FunctionDefinition MAX =
		new TempFunctionDefinition("max", AGGREGATE_FUNCTION);
	public static final FunctionDefinition MIN =
		new TempFunctionDefinition("min", AGGREGATE_FUNCTION);
	public static final FunctionDefinition SUM =
		new TempFunctionDefinition("sum", AGGREGATE_FUNCTION);
	public static final FunctionDefinition SUM0 =
		new TempFunctionDefinition("sum0", AGGREGATE_FUNCTION);
	public static final FunctionDefinition STDDEV_POP =
		new TempFunctionDefinition("stddevPop", AGGREGATE_FUNCTION);
	public static final FunctionDefinition STDDEV_SAMP =
		new TempFunctionDefinition("stddevSamp", AGGREGATE_FUNCTION);
	public static final FunctionDefinition VAR_POP =
		new TempFunctionDefinition("varPop", AGGREGATE_FUNCTION);
	public static final FunctionDefinition VAR_SAMP =
		new TempFunctionDefinition("varSamp", AGGREGATE_FUNCTION);
	public static final FunctionDefinition COLLECT =
		new TempFunctionDefinition("collect", AGGREGATE_FUNCTION);
	public static final FunctionDefinition DISTINCT =
		new TempFunctionDefinition("distinct", AGGREGATE_FUNCTION);

	// string functions
	public static final FunctionDefinition CHAR_LENGTH =
		new TempFunctionDefinition("charLength", SCALAR_FUNCTION);
	public static final FunctionDefinition INIT_CAP =
		new TempFunctionDefinition("initCap", SCALAR_FUNCTION);
	public static final FunctionDefinition LIKE =
		new TempFunctionDefinition("like", SCALAR_FUNCTION);
	public static final FunctionDefinition LOWER =
		new TempFunctionDefinition("lowerCase", SCALAR_FUNCTION);
	public static final FunctionDefinition SIMILAR =
		new TempFunctionDefinition("similar", SCALAR_FUNCTION);
	public static final FunctionDefinition SUBSTRING =
		new TempFunctionDefinition("substring", SCALAR_FUNCTION);
	public static final FunctionDefinition REPLACE =
		new TempFunctionDefinition("replace", SCALAR_FUNCTION);
	public static final FunctionDefinition TRIM =
		new TempFunctionDefinition("trim", SCALAR_FUNCTION);
	public static final FunctionDefinition UPPER =
		new TempFunctionDefinition("upperCase", SCALAR_FUNCTION);
	public static final FunctionDefinition POSITION =
		new TempFunctionDefinition("position", SCALAR_FUNCTION);
	public static final FunctionDefinition OVERLAY =
		new TempFunctionDefinition("overlay", SCALAR_FUNCTION);
	public static final FunctionDefinition CONCAT =
		new TempFunctionDefinition("concat", SCALAR_FUNCTION);
	public static final FunctionDefinition CONCAT_WS =
		new TempFunctionDefinition("concat_ws", SCALAR_FUNCTION);
	public static final FunctionDefinition LPAD =
		new TempFunctionDefinition("lpad", SCALAR_FUNCTION);
	public static final FunctionDefinition RPAD =
		new TempFunctionDefinition("rpad", SCALAR_FUNCTION);
	public static final FunctionDefinition REGEXP_EXTRACT =
		new TempFunctionDefinition("regexpExtract", SCALAR_FUNCTION);
	public static final FunctionDefinition FROM_BASE64 =
		new TempFunctionDefinition("fromBase64", SCALAR_FUNCTION);
	public static final FunctionDefinition TO_BASE64 =
		new TempFunctionDefinition("toBase64", SCALAR_FUNCTION);
	public static final FunctionDefinition UUID =
		new TempFunctionDefinition("uuid", SCALAR_FUNCTION);
	public static final FunctionDefinition LTRIM =
		new TempFunctionDefinition("ltrim", SCALAR_FUNCTION);
	public static final FunctionDefinition RTRIM =
		new TempFunctionDefinition("rtrim", SCALAR_FUNCTION);
	public static final FunctionDefinition REPEAT =
		new TempFunctionDefinition("repeat", SCALAR_FUNCTION);
	public static final FunctionDefinition REGEXP_REPLACE =
		new TempFunctionDefinition("regexpReplace", SCALAR_FUNCTION);

	// math functions
	public static final FunctionDefinition PLUS =
		new TempFunctionDefinition("plus", SCALAR_FUNCTION);
	public static final FunctionDefinition MINUS =
		new TempFunctionDefinition("minus", SCALAR_FUNCTION);
	public static final FunctionDefinition DIVIDE =
		new TempFunctionDefinition("divide", SCALAR_FUNCTION);
	public static final FunctionDefinition TIMES =
		new TempFunctionDefinition("times", SCALAR_FUNCTION);
	public static final FunctionDefinition ABS =
		new TempFunctionDefinition("abs", SCALAR_FUNCTION);
	public static final FunctionDefinition CEIL =
		new TempFunctionDefinition("ceil", SCALAR_FUNCTION);
	public static final FunctionDefinition EXP =
		new TempFunctionDefinition("exp", SCALAR_FUNCTION);
	public static final FunctionDefinition FLOOR =
		new TempFunctionDefinition("floor", SCALAR_FUNCTION);
	public static final FunctionDefinition LOG10 =
		new TempFunctionDefinition("log10", SCALAR_FUNCTION);
	public static final FunctionDefinition LOG2 =
		new TempFunctionDefinition("log2", SCALAR_FUNCTION);
	public static final FunctionDefinition LN =
		new TempFunctionDefinition("ln", SCALAR_FUNCTION);
	public static final FunctionDefinition LOG =
		new TempFunctionDefinition("log", SCALAR_FUNCTION);
	public static final FunctionDefinition POWER =
		new TempFunctionDefinition("power", SCALAR_FUNCTION);
	public static final FunctionDefinition MOD =
		new TempFunctionDefinition("mod", SCALAR_FUNCTION);
	public static final FunctionDefinition SQRT =
		new TempFunctionDefinition("sqrt", SCALAR_FUNCTION);
	public static final FunctionDefinition MINUS_PREFIX =
		new TempFunctionDefinition("minusPrefix", SCALAR_FUNCTION);
	public static final FunctionDefinition SIN =
		new TempFunctionDefinition("sin", SCALAR_FUNCTION);
	public static final FunctionDefinition COS =
		new TempFunctionDefinition("cos", SCALAR_FUNCTION);
	public static final FunctionDefinition SINH =
		new TempFunctionDefinition("sinh", SCALAR_FUNCTION);
	public static final FunctionDefinition TAN =
		new TempFunctionDefinition("tan", SCALAR_FUNCTION);
	public static final FunctionDefinition TANH =
		new TempFunctionDefinition("tanh", SCALAR_FUNCTION);
	public static final FunctionDefinition COT =
		new TempFunctionDefinition("cot", SCALAR_FUNCTION);
	public static final FunctionDefinition ASIN =
		new TempFunctionDefinition("asin", SCALAR_FUNCTION);
	public static final FunctionDefinition ACOS =
		new TempFunctionDefinition("acos", SCALAR_FUNCTION);
	public static final FunctionDefinition ATAN =
		new TempFunctionDefinition("atan", SCALAR_FUNCTION);
	public static final FunctionDefinition ATAN2 =
		new TempFunctionDefinition("atan2", SCALAR_FUNCTION);
	public static final FunctionDefinition COSH =
		new TempFunctionDefinition("cosh", SCALAR_FUNCTION);
	public static final FunctionDefinition DEGREES =
		new TempFunctionDefinition("degrees", SCALAR_FUNCTION);
	public static final FunctionDefinition RADIANS =
		new TempFunctionDefinition("radians", SCALAR_FUNCTION);
	public static final FunctionDefinition SIGN =
		new TempFunctionDefinition("sign", SCALAR_FUNCTION);
	public static final FunctionDefinition ROUND =
		new TempFunctionDefinition("round", SCALAR_FUNCTION);
	public static final FunctionDefinition PI =
		new TempFunctionDefinition("pi", SCALAR_FUNCTION);
	public static final FunctionDefinition E =
		new TempFunctionDefinition("e", SCALAR_FUNCTION);
	public static final FunctionDefinition RAND =
		new TempFunctionDefinition("rand", SCALAR_FUNCTION);
	public static final FunctionDefinition RAND_INTEGER =
		new TempFunctionDefinition("randInteger", SCALAR_FUNCTION);
	public static final FunctionDefinition BIN =
		new TempFunctionDefinition("bin", SCALAR_FUNCTION);
	public static final FunctionDefinition HEX =
		new TempFunctionDefinition("hex", SCALAR_FUNCTION);
	public static final FunctionDefinition TRUNCATE =
		new TempFunctionDefinition("truncate", SCALAR_FUNCTION);

	// time functions
	public static final FunctionDefinition EXTRACT =
		new TempFunctionDefinition("extract", SCALAR_FUNCTION);
	public static final FunctionDefinition CURRENT_DATE =
		new TempFunctionDefinition("currentDate", SCALAR_FUNCTION);
	public static final FunctionDefinition CURRENT_TIME =
		new TempFunctionDefinition("currentTime", SCALAR_FUNCTION);
	public static final FunctionDefinition CURRENT_TIMESTAMP =
		new TempFunctionDefinition("currentTimestamp", SCALAR_FUNCTION);
	public static final FunctionDefinition LOCAL_TIME =
		new TempFunctionDefinition("localTime", SCALAR_FUNCTION);
	public static final FunctionDefinition LOCAL_TIMESTAMP =
		new TempFunctionDefinition("localTimestamp", SCALAR_FUNCTION);
	public static final FunctionDefinition TEMPORAL_OVERLAPS =
		new TempFunctionDefinition("temporalOverlaps", SCALAR_FUNCTION);
	public static final FunctionDefinition DATE_TIME_PLUS =
		new TempFunctionDefinition("dateTimePlus", SCALAR_FUNCTION);
	public static final FunctionDefinition DATE_FORMAT =
		new TempFunctionDefinition("dateFormat", SCALAR_FUNCTION);
	public static final FunctionDefinition TIMESTAMP_DIFF =
		new TempFunctionDefinition("timestampDiff", SCALAR_FUNCTION);

	// collection
	public static final FunctionDefinition AT =
		new TempFunctionDefinition("at", SCALAR_FUNCTION);
	public static final FunctionDefinition CARDINALITY =
		new TempFunctionDefinition("cardinality", SCALAR_FUNCTION);
	public static final FunctionDefinition ARRAY =
		new TempFunctionDefinition("array", SCALAR_FUNCTION);
	public static final FunctionDefinition ARRAY_ELEMENT =
		new TempFunctionDefinition("element", SCALAR_FUNCTION);
	public static final FunctionDefinition MAP =
		new TempFunctionDefinition("map", SCALAR_FUNCTION);
	public static final FunctionDefinition ROW =
		new TempFunctionDefinition("row", SCALAR_FUNCTION);

	// composite
	public static final FunctionDefinition FLATTEN =
		new TempFunctionDefinition("flatten", OTHER_FUNCTION);
	public static final FunctionDefinition GET =
		new TempFunctionDefinition("get", OTHER_FUNCTION);

	// window properties
	public static final FunctionDefinition WINDOW_START =
		new TempFunctionDefinition("start", OTHER_FUNCTION);
	public static final FunctionDefinition WINDOW_END =
		new TempFunctionDefinition("end", OTHER_FUNCTION);

	// ordering
	public static final FunctionDefinition ORDER_ASC =
		new TempFunctionDefinition("asc", OTHER_FUNCTION);
	public static final FunctionDefinition ORDER_DESC =
		new TempFunctionDefinition("desc", OTHER_FUNCTION);

	// crypto hash
	public static final FunctionDefinition MD5 =
		new TempFunctionDefinition("md5", SCALAR_FUNCTION);
	public static final FunctionDefinition SHA1 =
		new TempFunctionDefinition("sha1", SCALAR_FUNCTION);
	public static final FunctionDefinition SHA224 =
		new TempFunctionDefinition("sha224", SCALAR_FUNCTION);
	public static final FunctionDefinition SHA256 =
		new TempFunctionDefinition("sha256", SCALAR_FUNCTION);
	public static final FunctionDefinition SHA384 =
		new TempFunctionDefinition("sha384", SCALAR_FUNCTION);
	public static final FunctionDefinition SHA512 =
		new TempFunctionDefinition("sha512", SCALAR_FUNCTION);
	public static final FunctionDefinition SHA2 =
		new TempFunctionDefinition("sha2", SCALAR_FUNCTION);

	// time attributes
	public static final FunctionDefinition PROCTIME =
		new TempFunctionDefinition("proctime", OTHER_FUNCTION);
	public static final FunctionDefinition ROWTIME =
		new TempFunctionDefinition("rowtime", OTHER_FUNCTION);

	// over window
	public static final FunctionDefinition OVER =
		new TempFunctionDefinition("over", OTHER_FUNCTION);
	public static final FunctionDefinition UNBOUNDED_RANGE =
		new TempFunctionDefinition("unboundedRange", OTHER_FUNCTION);
	public static final FunctionDefinition UNBOUNDED_ROW =
		new TempFunctionDefinition("unboundedRow", OTHER_FUNCTION);
	public static final FunctionDefinition CURRENT_RANGE =
		new TempFunctionDefinition("currentRange", OTHER_FUNCTION);
	public static final FunctionDefinition CURRENT_ROW =
		new TempFunctionDefinition("currentRow", OTHER_FUNCTION);

	// columns
	public static final FunctionDefinition WITH_COLUMNS =
		new TempFunctionDefinition("withColumns", OTHER_FUNCTION);
	public static final FunctionDefinition WITHOUT_COLUMNS =
		new TempFunctionDefinition("withoutColumns", OTHER_FUNCTION);

	// etc
	public static final FunctionDefinition IN =
		new TempFunctionDefinition("in", SCALAR_FUNCTION);
	public static final FunctionDefinition CAST =
		new TempFunctionDefinition("cast", SCALAR_FUNCTION);
	public static final FunctionDefinition REINTERPRET_CAST =
			new TempFunctionDefinition("reinterpretCast", SCALAR_FUNCTION);
	public static final FunctionDefinition AS =
		new TempFunctionDefinition("as", OTHER_FUNCTION);
	public static final FunctionDefinition STREAM_RECORD_TIMESTAMP =
		new TempFunctionDefinition("streamRecordTimestamp", OTHER_FUNCTION);
	public static final FunctionDefinition RANGE_TO =
		new TempFunctionDefinition("rangeTo", OTHER_FUNCTION);

	public static final Set<FunctionDefinition> WINDOW_PROPERTIES = new HashSet<>(Arrays.asList(
		WINDOW_START, WINDOW_END, PROCTIME, ROWTIME
	));

	public static final Set<FunctionDefinition> TIME_ATTRIBUTES = new HashSet<>(Arrays.asList(
		PROCTIME, ROWTIME
	));

	public static final List<FunctionDefinition> ORDERING = Arrays.asList(ORDER_ASC, ORDER_DESC);

	public static List<FunctionDefinition> getDefinitions() {
		final Field[] fields = BuiltInFunctionDefinitions.class.getFields();
		final List<FunctionDefinition> list = new ArrayList<>(fields.length);
		for (Field field : fields) {
			if (FunctionDefinition.class.isAssignableFrom(field.getType())) {
				try {
					final FunctionDefinition funcDef = (FunctionDefinition) field.get(BuiltInFunctionDefinitions.class);
					list.add(Preconditions.checkNotNull(funcDef));
				} catch (IllegalAccessException e) {
					throw new TableException(
						"The function definition for field " + field.getName() + " is not accessible.", e);
				}
			}
		}
		return list;
	}
}
