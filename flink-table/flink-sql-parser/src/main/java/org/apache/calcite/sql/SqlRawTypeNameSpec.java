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

package org.apache.calcite.sql;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.calcite.RawRelDataTypeFactory;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;

import java.util.Objects;

/**
 * Represents a raw type such as {@code RAW('org.my.Class', 'sW3Djsds...')}.
 *
 * <p>The raw type does not belong to standard SQL.
 */
@Internal
public final class SqlRawTypeNameSpec extends SqlTypeNameSpec {

	private static final String RAW_TYPE_NAME = "RAW";

	private final SqlCharStringLiteral className;

	private final SqlCharStringLiteral serializerString;

	public SqlRawTypeNameSpec(SqlCharStringLiteral className, SqlCharStringLiteral serializerString, SqlParserPos pos) {
		super(new SqlIdentifier(RAW_TYPE_NAME, pos), pos);
		this.className = className;
		this.serializerString = serializerString;
	}

	@Override
	public RelDataType deriveType(SqlValidator validator) {
		return ((RawRelDataTypeFactory) validator.getTypeFactory())
			.createRawType(className.toValue(), serializerString.toValue());
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword(RAW_TYPE_NAME);
		final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
		writer.sep(",");
		className.unparse(writer, leftPrec, rightPrec);
		writer.sep(",");
		serializerString.unparse(writer, leftPrec, rightPrec);
		writer.endList(frame);
	}

	@Override
	public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
		if (!(spec instanceof SqlRawTypeNameSpec)) {
			return litmus.fail("{} != {}", this, spec);
		}
		SqlRawTypeNameSpec that = (SqlRawTypeNameSpec) spec;
		if (!Objects.equals(this.className, that.className)) {
			return litmus.fail("{} != {}", this, spec);
		}
		if (!Objects.equals(this.serializerString, that.serializerString)) {
			return litmus.fail("{} != {}", this, spec);
		}
		return litmus.succeed();
	}
}
