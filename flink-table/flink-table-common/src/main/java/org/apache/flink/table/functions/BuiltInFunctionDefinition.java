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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.Preconditions;

/**
 * Definition of a built-in function.
 */
@Internal
public final class BuiltInFunctionDefinition implements FunctionDefinition {

	private final String name;

	private final FunctionKind kind;

	private final TypeInference typeInference;

	private BuiltInFunctionDefinition(
			String name,
			FunctionKind kind,
			TypeInference typeInference) {
		this.name = Preconditions.checkNotNull(name, "Name must not be null.");
		this.kind = Preconditions.checkNotNull(kind, "Kind must not be null.");
		this.typeInference = Preconditions.checkNotNull(typeInference, "Type inference must not be null.");
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public FunctionKind getKind() {
		return kind;
	}

	@Override
	public TypeInference getTypeInference() {
		return typeInference;
	}

	// --------------------------------------------------------------------------------------------

	public static class Builder {

		private String name;

		private FunctionKind kind;

		private TypeInference typeInference;

		public Builder() {
			// default constructor to allow a fluent definition
		}

		public Builder name(String name) {
			this.name = name;
			return this;
		}

		public Builder kind(FunctionKind kind) {
			this.kind = kind;
			return this;
		}

		public Builder typeInference(TypeInference typeInference) {
			this.typeInference = typeInference;
			return this;
		}

		public BuiltInFunctionDefinition build() {
			return new BuiltInFunctionDefinition(name, kind, typeInference);
		}
	}
}
