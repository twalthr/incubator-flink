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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser for creating instances of {@link LogicalType} from a serialized string created with
 * {@link LogicalType#asSerializableString()}.
 *
 * <p>In addition to the serializable string representations, this parser also supports common
 * shortcuts for certain types. This includes:
 * <ul>
 *     <li>{@code STRING} as a synonym for {@code VARCHAR(INT_MAX)}</li>
 *     <li>{@code BYTES} as a synonym for {@code VARBINARY(INT_MAX)}</li>
 *     <li>{@code NUMERIC} and {@code DEC} as synonyms for {@code DECIMAL}</li>
 *     <li>{@code INTEGER} as a synonym for {@code INT}</li>
 *     <li>{@code DOUBLE PRECISION} as a synonym for {@code DOUBLE}</li>
 *     <li>{@code TIME WITHOUT TIMEZONE} as a synonym for {@code TIME}</li>
 *     <li>{@code TIMESTAMP WITHOUT TIMEZONE} as a synonym for {@code TIMESTAMP}</li>
 *     <li>{@code type ARRAY} as a synonym for {@code ARRAY<type>}</li>
 *     <li>{@code type MULTISET} as a synonym for {@code MULTISET<type>}</li>
 *     <li>{@code ROW(...)} as a synonym for {@code ROW<...>}</li>
 *     <li>{@code type NULL} as a synonym for {@code type}</li>
 * </ul>
 */
@PublicEvolving
public final class LogicalTypeParser {

	public static LogicalType parse(String typeString) {
		final List<Token> tokens = tokenize(typeString);
		final TokenParser converter = new TokenParser(typeString, tokens);
		return converter.parseTokens();
	}

	// --------------------------------------------------------------------------------------------
	// Tokenizer
	// --------------------------------------------------------------------------------------------

	private static final char CHAR_BEGIN_SUBTYPE = '<';
	private static final char CHAR_END_SUBTYPE = '>';
	private static final char CHAR_BEGIN_PARAMETER = '(';
	private static final char CHAR_END_PARAMETER = ')';
	private static final char CHAR_LIST_SEPARATOR = ',';
	private static final char CHAR_STRING = '\'';
	private static final char CHAR_LITERAL = '`';
	private static final char CHAR_DOT = '.';

	private static boolean containsDelimiter(String string) {
		final char[] charArray = string.toCharArray();
		for (char c : charArray) {
			if (isDelimiter(c)) {
				return true;
			}
		}
		return false;
	}

	private static boolean isDelimiter(char character) {
		return Character.isWhitespace(character) ||
			character == CHAR_BEGIN_SUBTYPE ||
			character == CHAR_END_SUBTYPE ||
			character == CHAR_BEGIN_PARAMETER ||
			character == CHAR_END_PARAMETER ||
			character == CHAR_LIST_SEPARATOR ||
			character == CHAR_DOT;
	}

	private static List<Token> tokenize(String typeString) {
		final char[] chars = typeString.toCharArray();

		final List<Token> tokens = new ArrayList<>();
		final StringBuilder builder = new StringBuilder();
		for (int cursor = 0; cursor < chars.length; cursor++) {
			char curChar = chars[cursor];
			switch (curChar) {
				case CHAR_BEGIN_SUBTYPE:
					tokens.add(new Token(TokenType.BEGIN_SUBTYPE, Character.toString(curChar), cursor));
					break;
				case CHAR_END_SUBTYPE:
					tokens.add(new Token(TokenType.END_SUBTYPE, Character.toString(curChar), cursor));
					break;
				case CHAR_BEGIN_PARAMETER:
					tokens.add(new Token(TokenType.BEGIN_PARAMETER, Character.toString(curChar), cursor));
					break;
				case CHAR_END_PARAMETER:
					tokens.add(new Token(TokenType.END_PARAMETER, Character.toString(curChar), cursor));
					break;
				case CHAR_LIST_SEPARATOR:
					tokens.add(new Token(TokenType.LIST_SEPARATOR, Character.toString(curChar), cursor));
					break;
				case CHAR_DOT:
					tokens.add(new Token(TokenType.DOT, Character.toString(curChar), cursor));
					break;
				case CHAR_STRING:
					builder.setLength(0);
					cursor = consumeString(builder, chars, cursor);
					tokens.add(new Token(TokenType.STRING, builder.toString(), cursor));
					break;
				default:
					if (!Character.isWhitespace(curChar)) {
						builder.setLength(0);
						cursor = consumeLiteral(builder, chars, cursor);
						tokens.add(new Token(TokenType.LITERAL, builder.toString(), cursor));
					}
			}
		}

		return tokens;
	}

	private static int consumeString(StringBuilder builder, char[] chars, int cursor) {
		while (chars.length > cursor) {
			final char curChar = chars[cursor++];
			if (curChar == CHAR_STRING && cursor < chars.length && chars[cursor] == CHAR_STRING) {
				// escaping of the escaping char e.g. "'Hello '' World'"
				cursor++;
				builder.append(curChar);
			} else if (curChar == CHAR_STRING) {
				break;
			} else {
				builder.append(curChar);
			}
		}
		return cursor;
	}

	private static int consumeLiteral(StringBuilder builder, char[] chars, int cursor) {
		boolean isEscaped = false;
		while (cursor < chars.length && (!isDelimiter(chars[cursor]) || isEscaped)) {
			final char curChar = chars[cursor++];
			if (!isEscaped && curChar == CHAR_LITERAL) {
				isEscaped = true;
			} else if (isEscaped && curChar == CHAR_LITERAL &&
					cursor < chars.length && chars[cursor] == CHAR_LITERAL) {
				// escaping of the escaping char e.g. "`Hello `` World`"
				cursor++;
				builder.append(curChar);
			} else if (isEscaped && curChar == CHAR_LITERAL) {
				break;
			} else {
				builder.append(curChar);
			}
		}
		return cursor - 1;
	}

	private enum TokenType {
		// e.g. "ROW<"
		BEGIN_SUBTYPE,

		// e.g. "ROW<..>"
		END_SUBTYPE,

		// e.g. "CHAR("
		BEGIN_PARAMETER,

		// e.g. "CHAR(...)"
		END_PARAMETER,

		// e.g. "ROW<INT,"
		LIST_SEPARATOR,

		// e.g. "ROW<name INT 'Comment'"
		STRING,

		// e.g. "ROW<`name`" or "CHAR" or "CHAR(12"
		LITERAL,

		// e.g. "myCatalog.myDatabase."
		DOT
	}

	private static class Token {
		public final TokenType type;
		public final String literal;
		public final int cursorPosition;

		public Token(TokenType type, String literal, int cursorPosition) {
			this.type = type;
			this.literal = literal;
			this.cursorPosition = cursorPosition;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Token Parsing
	// --------------------------------------------------------------------------------------------

	private static final String LITERAL_NOT = "NOT";
	private static final String LITERAL_NULL = "NULL";
	private static final String LITERAL_CHAR = "CHAR";
	private static final String LITERAL_VARCHAR = "VARCHAR";
	private static final String LITERAL_STRING = "STRING";
	private static final String LITERAL_BOOLEAN = "BOOLEAN";
	private static final String LITERAL_BINARY = "BINARY";
	private static final String LITERAL_VARBINARY = "VARBINARY";
	private static final String LITERAL_BYTES = "BYTES";
	private static final String LITERAL_DECIMAL = "DECIMAL";
	private static final String LITERAL_DEC = "DEC";
	private static final String LITERAL_NUMERIC = "NUMERIC";
	private static final String LITERAL_INT = "INT";
	private static final String LITERAL_INTEGER = "INTEGER";
	private static final String LITERAL_TINYINT = "TINYINT";
	private static final String LITERAL_SMALLINT = "SMALLINT";
	private static final String LITERAL_BIGINT = "BIGINT";
	private static final String LITERAL_FLOAT = "FLOAT";
	private static final String LITERAL_DOUBLE = "DOUBLE";
	private static final String LITERAL_PRECISION = "PRECISION";
	private static final String LITERAL_DATE = "DATE";
	private static final String LITERAL_TIME = "TIME";
	private static final String LITERAL_TIMESTAMP = "TIMESTAMP";
	private static final String LITERAL_WITH = "WITH";
	private static final String LITERAL_WITHOUT = "WITHOUT";
	private static final String LITERAL_ZONE = "ZONE";
	private static final String LITERAL_LOCAL = "LOCAL";
	private static final String LITERAL_INTERVAL = "INTERVAL";
	private static final String LITERAL_YEAR = "YEAR";
	private static final String LITERAL_MONTH = "MONTH";
	private static final String LITERAL_DAY = "DAY";
	private static final String LITERAL_HOUR = "HOUR";
	private static final String LITERAL_MINUTE = "MINUTE";
	private static final String LITERAL_SECOND = "SECOND";
	private static final String LITERAL_TO = "TO";

	private static class TokenParser {

		private String inputString;
		private List<Token> tokens;
		private int lastValidToken;
		private int currentToken;

		public TokenParser(String inputString, List<Token> tokens) {
			this.inputString = inputString;
			this.tokens = tokens;
			this.lastValidToken = -1;
			this.currentToken = -1;
		}

		public LogicalType parseTokens() {
			final LogicalType type = parseType();
			if (hasRemainingTokens()) {
				nextToken();
				throw parsingError("Unexpected token: " + token().literal);
			}
			return type;
		}

		private void nextToken(TokenType type) {
			nextToken(type, null);
		}

		private void nextToken(TokenType type, @Nullable String literal) {
			nextToken();
			final Token nextToken = tokens.get(currentToken);
			if (nextToken.type != type) {
				throw parsingError(type.name() + " expected but was " + nextToken.type + '.');
			}
			if (literal != null && !nextToken.literal.equals(literal)) {
				throw parsingError("'" + literal + "' expected but was '" + nextToken.literal + "'.");
			}
		}

		private void nextToken() {
			this.currentToken++;
			if (currentToken >= tokens.size()) {
				throw parsingError("Unexpected end.");
			}
			lastValidToken = this.currentToken - 1;
		}

		private boolean isNextToken(int lookAhead, TokenType type) {
			return currentToken + lookAhead < tokens.size() &&
				tokens.get(currentToken + lookAhead).type == type;
		}

		private boolean isNextToken(int lookAhead, TokenType type, @Nullable String literal) {
			if (currentToken + lookAhead >= tokens.size()) {
				return false;
			}
			final Token nextToken = tokens.get(currentToken + lookAhead);
			return nextToken.type == type && (literal == null || literal.equals(nextToken.literal));
		}

		private int lastCursor() {
			if (lastValidToken < 0) {
				return 0;
			}
			return tokens.get(lastValidToken).cursorPosition + 1;
		}

		private ValidationException parsingError(String cause) {
			throw new ValidationException("Could not parse type at position " + lastCursor() + ": " + cause + "\n" +
				"Input type string: " + inputString);
		}

		private Token token() {
			return tokens.get(currentToken);
		}

		private boolean hasRemainingTokens() {
			return currentToken + 1 < tokens.size();
		}

		private int parseInt() {
			nextToken(TokenType.LITERAL);
			return Integer.valueOf(token().literal);
		}

		private boolean parseNullability() {
			// "NOT NULL"
			if (isNextToken(1, TokenType.LITERAL) && isNextToken(2, TokenType.LITERAL)) {
				nextToken(TokenType.LITERAL, LITERAL_NOT);
				nextToken(TokenType.LITERAL, LITERAL_NULL);
				return false;
			}
			// explicit "NULL"
			else if (isNextToken(1, TokenType.LITERAL)) {
				nextToken(TokenType.LITERAL, LITERAL_NULL);
				return true;
			}
			// implicit "NULL"
			return true;
		}

		private LogicalType parseType() {
			nextToken(TokenType.LITERAL);
			final LogicalType type;
			switch (token().literal) {
				case LITERAL_CHAR:
					type = parseCharType();
					break;
				case LITERAL_VARCHAR:
					type = parseVarCharType();
					break;
				case LITERAL_STRING:
					type = new VarCharType(VarCharType.MAX_LENGTH);
					break;
				case LITERAL_BOOLEAN:
					type = new BooleanType();
					break;
				case LITERAL_BINARY:
					type = parseBinaryType();
					break;
				case LITERAL_VARBINARY:
					type = parseVarBinaryType();
					break;
				case LITERAL_BYTES:
					type = new VarBinaryType(VarBinaryType.MAX_LENGTH);
					break;
				case LITERAL_DECIMAL:
				case LITERAL_DEC:
				case LITERAL_NUMERIC:
					type = parseDecimalType();
					break;
				case LITERAL_TINYINT:
					type = new TinyIntType();
					break;
				case LITERAL_SMALLINT:
					type = new SmallIntType();
					break;
				case LITERAL_INTEGER:
				case LITERAL_INT:
					type = new IntType();
					break;
				case LITERAL_BIGINT:
					type = new BigIntType();
					break;
				case LITERAL_FLOAT:
					type = new FloatType();
					break;
				case LITERAL_DOUBLE:
					type = parseDoubleType();
					break;
				case LITERAL_DATE:
					type = new DateType();
					break;
				case LITERAL_TIME:
					type = parseTimeType();
					break;
				case LITERAL_TIMESTAMP:
					type = parseTimestampType();
					break;
				case LITERAL_INTERVAL:
					type = parseIntervalType();
					break;
				default:
					throw parsingError("Unsupported type: " + token().literal);
			}

			return type.copy(parseNullability());
		}

		private LogicalType parseCharType() {
			// "CHAR(length)"
			if (isNextToken(1, TokenType.BEGIN_PARAMETER)) {
				nextToken(TokenType.BEGIN_PARAMETER);
				final int length = parseInt();
				nextToken(TokenType.END_PARAMETER);
				return new CharType(length);
			}
			// "CHAR"
			return new CharType();
		}

		private LogicalType parseVarCharType() {
			// "VARCHAR(length)"
			if (isNextToken(1, TokenType.BEGIN_PARAMETER)) {
				nextToken(TokenType.BEGIN_PARAMETER);
				final int length = parseInt();
				nextToken(TokenType.END_PARAMETER);
				return new VarCharType(length);
			}
			// "VARCHAR"
			return new VarCharType();
		}

		private LogicalType parseBinaryType() {
			// "BINARY(length)"
			if (isNextToken(1, TokenType.BEGIN_PARAMETER)) {
				nextToken(TokenType.BEGIN_PARAMETER);
				final int length = parseInt();
				nextToken(TokenType.END_PARAMETER);
				return new BinaryType(length);
			}
			// "BINARY"
			return new BinaryType();
		}

		private LogicalType parseVarBinaryType() {
			// "VARBINARY(length)"
			if (isNextToken(1, TokenType.BEGIN_PARAMETER)) {
				nextToken(TokenType.BEGIN_PARAMETER);
				final int length = parseInt();
				nextToken(TokenType.END_PARAMETER);
				return new VarBinaryType(length);
			}
			// "VARBINARY"
			return new VarBinaryType();
		}

		private LogicalType parseDecimalType() {
			if (isNextToken(1, TokenType.BEGIN_PARAMETER)) {
				nextToken(TokenType.BEGIN_PARAMETER);
				final int precision = parseInt();
				int scale = DecimalType.DEFAULT_SCALE;
				// "DECIMAL(precision, scale)", "DEC(precision, scale)", "NUMERIC(precision, scale)"
				if (isNextToken(1, TokenType.LIST_SEPARATOR)) {
					nextToken(TokenType.LIST_SEPARATOR);
					scale = parseInt();
				}
				nextToken(TokenType.END_PARAMETER);
				return new DecimalType(precision, scale);
			}
			// "DECIMAL", "DEC", "NUMERIC"
			return new DecimalType();
		}

		private LogicalType parseDoubleType() {
			// "DOUBLE PRECISION"
			if (isNextToken(1, TokenType.LITERAL, LITERAL_PRECISION)) {
				nextToken(TokenType.LITERAL, LITERAL_PRECISION);
			}
			// "DOUBLE"
			return new DoubleType();
		}

		private LogicalType parseTimeType() {
			// "TIME"
			int precision = TimeType.DEFAULT_PRECISION;
			// "TIME(precision)"
			if (isNextToken(1, TokenType.BEGIN_PARAMETER)) {
				nextToken(TokenType.BEGIN_PARAMETER);
				precision = parseInt();
				nextToken(TokenType.END_PARAMETER);
			}
			// "TIME(precision) WITHOUT TIME ZONE"
			if (isNextToken(1, TokenType.LITERAL, LITERAL_WITHOUT)) {
				nextToken(TokenType.LITERAL, LITERAL_WITHOUT);
				nextToken(TokenType.LITERAL, LITERAL_TIME);
				nextToken(TokenType.LITERAL, LITERAL_ZONE);
			}
			return new TimeType(precision);
		}

		private LogicalType parseTimestampType() {
			int precision = TimestampType.DEFAULT_PRECISION;
			// "TIMESTAMP(precision)"
			if (isNextToken(1, TokenType.BEGIN_PARAMETER)) {
				nextToken(TokenType.BEGIN_PARAMETER);
				precision = parseInt();
				nextToken(TokenType.END_PARAMETER);
			}
			// "TIMESTAMP" or "TIMESTAMP(precision)"
			if (!isNextToken(1, TokenType.LITERAL)) {
				return new TimestampType(precision);
			}
			// "TIMESTAMP(precision) WITHOUT TIME ZONE"
			if (isNextToken(1, TokenType.LITERAL, LITERAL_WITHOUT)) {
				nextToken(TokenType.LITERAL, LITERAL_WITHOUT);
				nextToken(TokenType.LITERAL, LITERAL_TIME);
				nextToken(TokenType.LITERAL, LITERAL_ZONE);
				return new TimestampType(precision);
			}
			// "TIMESTAMP(precision) WITH
			else if (isNextToken(1, TokenType.LITERAL, LITERAL_WITH)) {
				nextToken(TokenType.LITERAL, LITERAL_WITH);
				// "TIMESTAMP(precision) WITH LOCAL TIME ZONE"
				if (isNextToken(1, TokenType.LITERAL, LITERAL_LOCAL)) {
					nextToken(TokenType.LITERAL, LITERAL_LOCAL);
					nextToken(TokenType.LITERAL, LITERAL_TIME);
					nextToken(TokenType.LITERAL, LITERAL_ZONE);
					return new LocalZonedTimestampType(precision);
				}
				// "TIMESTAMP(precision) WITH TIME ZONE"
				else {
					nextToken(TokenType.LITERAL, LITERAL_TIME);
					nextToken(TokenType.LITERAL, LITERAL_ZONE);
					return new ZonedTimestampType(precision);
				}
			}
			return new TimestampType(precision);
		}

		private LogicalType parseIntervalType() {
			nextToken(TokenType.LITERAL);

			switch (token().literal) {
				case LITERAL_YEAR:
				case LITERAL_MONTH:
					return parseYearMonthIntervalType();
				case LITERAL_DAY:
				case LITERAL_HOUR:
				case LITERAL_MINUTE:
				case LITERAL_SECOND:
					return parseDayTimeIntervalType();
				default:
					parsingError("Invalid interval resolution.");
			}
			throw new RuntimeException();
		}

		private LogicalType parseYearMonthIntervalType() {
			switch (token().literal) {
				case LITERAL_YEAR:
					int precision = YearMonthIntervalType.DEFAULT_PRECISION;
					// "INTERVAL YEAR(precision)"
					if (isNextToken(1, TokenType.BEGIN_PARAMETER)) {
						nextToken(TokenType.BEGIN_PARAMETER);
						precision = parseInt();
						nextToken(TokenType.END_PARAMETER);
					}
					// "INTERVAL YEAR TO MONTH"
					if (isNextToken(1, TokenType.LITERAL, LITERAL_TO)) {
						nextToken(TokenType.LITERAL, LITERAL_TO);
						nextToken(TokenType.LITERAL, LITERAL_MONTH);
						return new YearMonthIntervalType(YearMonthResolution.YEAR_TO_MONTH, precision);
					}
					// "INTERVAL YEAR"
					return new YearMonthIntervalType(YearMonthResolution.YEAR, precision);
				case LITERAL_MONTH:
					// "INTERVAL MONTH"
					return new YearMonthIntervalType(YearMonthResolution.MONTH);
				default:
					parsingError("Invalid interval resolution.");
			}
			throw new RuntimeException();
		}

		private LogicalType parseDayTimeIntervalType() {
			switch (token().literal) {
				case LITERAL_DAY:
					int precision = YearMonthIntervalType.DEFAULT_PRECISION;
			}
			throw new RuntimeException();
		}
	}
}
