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

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

/**
 * Dictionary of function definitions for all built-in functions.
 */
@PublicEvolving
public final class BuiltInFunctionDefinitions {

	// logic functions
	public static final FunctionDefinition AND = new FunctionDefinition("and");
	public static final FunctionDefinition OR = new FunctionDefinition("or");
	public static final FunctionDefinition NOT = new FunctionDefinition("not");
	public static final FunctionDefinition IF = new FunctionDefinition("if");

	// comparison functions
	public static final FunctionDefinition EQUALS = new FunctionDefinition("equals");
	public static final FunctionDefinition GREATER_THAN = new FunctionDefinition("greaterThan");
	public static final FunctionDefinition GREATER_THAN_OR_EQUAL =
		new FunctionDefinition("greaterThanOrEqual");
	public static final FunctionDefinition LESS_THAN = new FunctionDefinition("lessThan");
	public static final FunctionDefinition LESS_THAN_OR_EQUAL =
		new FunctionDefinition("lessThanOrEqual");
	public static final FunctionDefinition NOT_EQUALS = new FunctionDefinition("notEquals");
	public static final FunctionDefinition IS_NULL = new FunctionDefinition("isNull");
	public static final FunctionDefinition IS_NOT_NULL = new FunctionDefinition("isNotNull");
	public static final FunctionDefinition IS_TRUE = new FunctionDefinition("isTrue");
	public static final FunctionDefinition IS_FALSE = new FunctionDefinition("isFalse");
	public static final FunctionDefinition IS_NOT_TRUE = new FunctionDefinition("isNotTrue");
	public static final FunctionDefinition IS_NOT_FALSE = new FunctionDefinition("isNotFalse");
	public static final FunctionDefinition BETWEEN = new FunctionDefinition("between");
	public static final FunctionDefinition NOT_BETWEEN = new FunctionDefinition("notBetween");

	// aggregate functions
	public static final FunctionDefinition AVG = new FunctionDefinition("avg");
	public static final FunctionDefinition COUNT = new FunctionDefinition("count");
	public static final FunctionDefinition MAX = new FunctionDefinition("max");
	public static final FunctionDefinition MIN = new FunctionDefinition("min");
	public static final FunctionDefinition SUM = new FunctionDefinition("sum");
	public static final FunctionDefinition SUM0 = new FunctionDefinition("sum0");
	public static final FunctionDefinition STDDEV_POP = new FunctionDefinition("stddevPop");
	public static final FunctionDefinition STDDEV_SAMP = new FunctionDefinition("stddevSamp");
	public static final FunctionDefinition VAR_POP = new FunctionDefinition("varPop");
	public static final FunctionDefinition VAR_SAMP = new FunctionDefinition("varSamp");
	public static final FunctionDefinition COLLECT = new FunctionDefinition("collect");
	public static final FunctionDefinition DISTINCT = new FunctionDefinition("distinct");

	// string functions
	public static final FunctionDefinition CHAR_LENGTH = new FunctionDefinition("charLength");
	public static final FunctionDefinition INIT_CAP = new FunctionDefinition("initCap");
	public static final FunctionDefinition LIKE = new FunctionDefinition("like");
	public static final FunctionDefinition LOWER = new FunctionDefinition("lower");
	public static final FunctionDefinition SIMILAR = new FunctionDefinition("similar");
	public static final FunctionDefinition SUBSTRING = new FunctionDefinition("substring");
	public static final FunctionDefinition REPLACE = new FunctionDefinition("replace");
	public static final FunctionDefinition TRIM = new FunctionDefinition("trim");
	public static final FunctionDefinition UPPER = new FunctionDefinition("upper");
	public static final FunctionDefinition POSITION = new FunctionDefinition("position");
	public static final FunctionDefinition OVERLAY = new FunctionDefinition("overlay");
	public static final FunctionDefinition CONCAT = new FunctionDefinition("concat");
	public static final FunctionDefinition CONCAT_WS = new FunctionDefinition("concat_ws");
	public static final FunctionDefinition LPAD = new FunctionDefinition("lpad");
	public static final FunctionDefinition RPAD = new FunctionDefinition("rpad");
	public static final FunctionDefinition REGEXP_EXTRACT = new FunctionDefinition("regexpExtract");
	public static final FunctionDefinition FROM_BASE64 = new FunctionDefinition("fromBase64");
	public static final FunctionDefinition TO_BASE64 = new FunctionDefinition("toBase64");
	public static final FunctionDefinition UUID = new FunctionDefinition("uuid");
	public static final FunctionDefinition LTRIM = new FunctionDefinition("ltrim");
	public static final FunctionDefinition RTRIM = new FunctionDefinition("rtrim");
	public static final FunctionDefinition REPEAT = new FunctionDefinition("repeat");
	public static final FunctionDefinition REGEXP_REPLACE = new FunctionDefinition("regexpReplace");

	// math functions
	public static final FunctionDefinition PLUS = new FunctionDefinition("plus");
	public static final FunctionDefinition MINUS = new FunctionDefinition("minus");
	public static final FunctionDefinition DIVIDE = new FunctionDefinition("divide");
	public static final FunctionDefinition TIMES = new FunctionDefinition("times");
	public static final FunctionDefinition ABS = new FunctionDefinition("abs");
	public static final FunctionDefinition CEIL = new FunctionDefinition("ceil");
	public static final FunctionDefinition EXP = new FunctionDefinition("exp");
	public static final FunctionDefinition FLOOR = new FunctionDefinition("floor");
	public static final FunctionDefinition LOG10 = new FunctionDefinition("log10");
	public static final FunctionDefinition LOG2 = new FunctionDefinition("log2");
	public static final FunctionDefinition LN = new FunctionDefinition("ln");
	public static final FunctionDefinition LOG = new FunctionDefinition("log");
	public static final FunctionDefinition POWER = new FunctionDefinition("power");
	public static final FunctionDefinition MOD = new FunctionDefinition("mod");
	public static final FunctionDefinition SQRT = new FunctionDefinition("sqrt");
	public static final FunctionDefinition MINUS_PREFIX = new FunctionDefinition("minusPrefix");
	public static final FunctionDefinition SIN = new FunctionDefinition("sin");
	public static final FunctionDefinition COS = new FunctionDefinition("cos");
	public static final FunctionDefinition SINH = new FunctionDefinition("sinh");
	public static final FunctionDefinition TAN = new FunctionDefinition("tan");
	public static final FunctionDefinition TANH = new FunctionDefinition("tanh");
	public static final FunctionDefinition COT = new FunctionDefinition("cot");
	public static final FunctionDefinition ASIN = new FunctionDefinition("asin");
	public static final FunctionDefinition ACOS = new FunctionDefinition("acos");
	public static final FunctionDefinition ATAN = new FunctionDefinition("atan");
	public static final FunctionDefinition ATAN2 = new FunctionDefinition("atan2");
	public static final FunctionDefinition COSH = new FunctionDefinition("cosh");
	public static final FunctionDefinition DEGREES = new FunctionDefinition("degrees");
	public static final FunctionDefinition RADIANS = new FunctionDefinition("radians");
	public static final FunctionDefinition SIGN = new FunctionDefinition("sign");
	public static final FunctionDefinition ROUND = new FunctionDefinition("round");
	public static final FunctionDefinition PI = new FunctionDefinition("pi");
	public static final FunctionDefinition E = new FunctionDefinition("e");
	public static final FunctionDefinition RAND = new FunctionDefinition("rand");
	public static final FunctionDefinition RAND_INTEGER = new FunctionDefinition("randInteger");
	public static final FunctionDefinition BIN = new FunctionDefinition("bin");
	public static final FunctionDefinition HEX = new FunctionDefinition("hex");
	public static final FunctionDefinition TRUNCATE = new FunctionDefinition("truncate");

	// time functions
	public static final FunctionDefinition EXTRACT = new FunctionDefinition("extract");
	public static final FunctionDefinition CURRENT_DATE = new FunctionDefinition("currentDate");
	public static final FunctionDefinition CURRENT_TIME = new FunctionDefinition("currentTime");
	public static final FunctionDefinition CURRENT_TIMESTAMP = new FunctionDefinition("currentTimestamp");
	public static final FunctionDefinition LOCAL_TIME = new FunctionDefinition("localTime");
	public static final FunctionDefinition LOCAL_TIMESTAMP = new FunctionDefinition("localTimestamp");
	public static final FunctionDefinition TEMPORAL_OVERLAPS = new FunctionDefinition("temporalOverlaps");
	public static final FunctionDefinition DATE_TIME_PLUS = new FunctionDefinition("dateTimePlus");
	public static final FunctionDefinition DATE_FORMAT = new FunctionDefinition("dateFormat");
	public static final FunctionDefinition TIMESTAMP_DIFF = new FunctionDefinition("timestampDiff");
	public static final FunctionDefinition TEMPORAL_FLOOR = new FunctionDefinition("temporalFloor");
	public static final FunctionDefinition TEMPORAL_CEIL = new FunctionDefinition("temporalCeil");

	// collection
	public static final FunctionDefinition AT = new FunctionDefinition("at");
	public static final FunctionDefinition CARDINALITY = new FunctionDefinition("cardinality");
	public static final FunctionDefinition ARRAY = new FunctionDefinition("array");
	public static final FunctionDefinition ARRAY_ELEMENT = new FunctionDefinition("element");
	public static final FunctionDefinition MAP = new FunctionDefinition("map");
	public static final FunctionDefinition ROW = new FunctionDefinition("row");

	// composite
	public static final FunctionDefinition FLATTEN = new FunctionDefinition("flatten");
	public static final FunctionDefinition GET = new FunctionDefinition("get");

	// window properties
	public static final FunctionDefinition WIN_START = new FunctionDefinition("start");
	public static final FunctionDefinition WIN_END = new FunctionDefinition("end");

	// ordering
	public static final FunctionDefinition ASC = new FunctionDefinition("asc");
	public static final FunctionDefinition DESC = new FunctionDefinition("desc");

	// crypto hash
	public static final FunctionDefinition MD5 = new FunctionDefinition("md5");
	public static final FunctionDefinition SHA1 = new FunctionDefinition("sha1");
	public static final FunctionDefinition SHA224 = new FunctionDefinition("sha224");
	public static final FunctionDefinition SHA256 = new FunctionDefinition("sha256");
	public static final FunctionDefinition SHA384 = new FunctionDefinition("sha384");
	public static final FunctionDefinition SHA512 = new FunctionDefinition("sha512");
	public static final FunctionDefinition SHA2 = new FunctionDefinition("sha2");

	// time attributes
	public static final FunctionDefinition PROCTIME = new FunctionDefinition("proctime");
	public static final FunctionDefinition ROWTIME = new FunctionDefinition("rowtime");

	// over window
	public static final FunctionDefinition OVER_CALL = new FunctionDefinition("overCall");
	public static final FunctionDefinition UNBOUNDED_RANGE = new FunctionDefinition("unboundedRange");
	public static final FunctionDefinition UNBOUNDED_ROW = new FunctionDefinition("unboundedRow");
	public static final FunctionDefinition CURRENT_RANGE = new FunctionDefinition("currentRange");
	public static final FunctionDefinition CURRENT_ROW = new FunctionDefinition("currentRow");

	// other functions
	public static final FunctionDefinition STREAM_RECORD_TIMESTAMP =
		new FunctionDefinition("streamRecordTimestamp");
	public static final FunctionDefinition IN = new FunctionDefinition("in");
	public static final FunctionDefinition CAST = new FunctionDefinition("cast");
	public static final FunctionDefinition AS = new FunctionDefinition("as");

	public static List<FunctionDefinition> getDefinitions() {
		List<FunctionDefinition> list = new LinkedList<>();
		for (Field field : BuiltInFunctionDefinitions.class.getFields()) {
			if (FunctionDefinition.class.isAssignableFrom(field.getType())) {
				try {
					FunctionDefinition funcDef = (FunctionDefinition) field.get(BuiltInFunctionDefinitions.class);
					list.add(funcDef);
				} catch (IllegalAccessException e) {
					throw new TableException(
						"The function definition for field " + field.getName() + " is not accessible.", e);
				}
			}
		}
		return list;
	}
}
