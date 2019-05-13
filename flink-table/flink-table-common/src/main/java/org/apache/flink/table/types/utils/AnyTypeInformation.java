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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.types.logical.AnyType;
import org.apache.flink.util.Preconditions;

/**
 * Type information for the {@link AnyType}.
 *
 * <p>This allows to represent serialized data as an instance of {@link TypeInformation}.
 */
@PublicEvolving
public final class AnyTypeInformation<T> extends TypeInformation<T> {

	private final AnyType<T> anyType;

	AnyTypeInformation(AnyType<T> anyType) {
		this.anyType = Preconditions.checkNotNull(anyType, "Any type must not be null");
	}

	public AnyType<T> getAnyType() {
		return anyType;
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<T> getTypeClass() {
		return anyType.getOriginatingClass();
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		return anyType.getTypeSerializer();
	}

	@Override
	public String toString() {
		return anyType.asSummaryString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		AnyTypeInformation<?> that = (AnyTypeInformation<?>) o;
		return anyType.equals(that.anyType);
	}

	@Override
	public int hashCode() {
		return anyType.hashCode();
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof AnyTypeInformation;
	}
}
