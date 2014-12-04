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

package org.apache.flink.api.common.typeutils.base;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.FloatValue;


public class FloatValueSerializer extends TypeSerializerSingleton<FloatValue> {

	private static final long serialVersionUID = 1L;

	public static final FloatValueSerializer INSTANCE = new FloatValueSerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public boolean isStateful() {
		return false;
	}

	@Override
	public boolean canCreateInstance() {
		return true;
	}

	@Override
	public FloatValue createInstance() {
		return new FloatValue();
	}

	@Override
	public FloatValue copy(FloatValue from) {
		FloatValue result = new FloatValue();
		result.setValue(from.getValue());
		return result;
	}

	@Override
	public FloatValue copy(FloatValue from, FloatValue reuse) {
		if (reuse == null) {
			reuse = copy(from);
		} else {
			reuse.setValue(from.getValue());
		}
		return reuse;
	}

	@Override
	public int getLength() {
		return 4;
	}

	@Override
	public void serialize(FloatValue record, DataOutputView target) throws IOException {
		record.write(target);
	}

	@Override
	public FloatValue deserialize(DataInputView source) throws IOException {
		FloatValue result = new FloatValue();
		result.read(source);
		return result;
	}

	@Override
	public FloatValue deserialize(FloatValue reuse, DataInputView source) throws IOException {
		if (reuse == null) {
			reuse = deserialize(source);
		}
		else {
			reuse.read(source);
		}
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeFloat(source.readFloat());
	}
}
