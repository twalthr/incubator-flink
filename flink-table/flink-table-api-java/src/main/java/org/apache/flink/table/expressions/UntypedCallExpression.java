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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A call expression where the target function has not been resolved yet.
 */
@Internal
public final class UntypedCallExpression implements Expression {

	private final @Nullable ObjectIdentifier objectIdentifier;

	private final FunctionDefinition functionDefinition;

	private final List<Expression> args;

	public UntypedCallExpression(
			ObjectIdentifier objectIdentifier,
			FunctionDefinition functionDefinition,
			List<Expression> args) {
		this.objectIdentifier = Preconditions.checkNotNull(objectIdentifier, "Object identifier must not be null.");
		this.functionDefinition = Preconditions.checkNotNull(functionDefinition, "Function definition must not be null.");
		this.args = new ArrayList<>(Preconditions.checkNotNull(args, "Arguments must not be null."));
	}

	public UntypedCallExpression(
			FunctionDefinition functionDefinition,
			List<Expression> args) {
		this.objectIdentifier = null;
		this.functionDefinition = Preconditions.checkNotNull(functionDefinition, "Function definition must not be null.");
		this.args = new ArrayList<>(Preconditions.checkNotNull(args, "Arguments must not be null."));
	}

	public UntypedCallExpression replaceArgs(List<Expression> args) {
		if (objectIdentifier == null) {
			return new UntypedCallExpression(functionDefinition, args);
		}
		return new UntypedCallExpression(objectIdentifier, functionDefinition, args);
	}

	public CallExpression resolve(List<ResolvedExpression> args, DataType dataType) {
		if (objectIdentifier == null) {
			return new CallExpression(
				functionDefinition,
				args,
				dataType);
		}
		return new CallExpression(
			objectIdentifier,
			functionDefinition,
			args,
			dataType);
	}

	public Optional<ObjectIdentifier> getObjectIdentifier() {
		return Optional.ofNullable(objectIdentifier);
	}

	public FunctionDefinition getFunctionDefinition() {
		return functionDefinition;
	}

	public String getFunctionSummary() {
		if (objectIdentifier == null) {
			return "*" + functionDefinition.toString() + "*";
		} else {
			return objectIdentifier.asSerializableString();
		}
	}

	@Override
	public String asSummaryString() {
		final List<String> argList = args.stream().map(Object::toString).collect(Collectors.toList());
		return getFunctionSummary() + "(" + String.join(", ", argList) + ")";
	}

	@Override
	public List<Expression> getChildren() {
		return args;
	}

	@Override
	public <R> R accept(ExpressionVisitor<R> visitor) {
		return visitor.visit(this);
	}
}
