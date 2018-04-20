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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.EvolutionUser;
import org.apache.flink.formats.avro.generated.Priority;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.formats.avro.utils.AvroKryoSerializerUtils;

import org.apache.commons.collections.map.HashedMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This test checks if we can restore from an Avro class that has undergone a schema change, i.e. we
 * removed field 'age' and added the field 'address'.
 */
public class AvroSchemaEvolutionTest {

	private static final String SNAPSHOT_RESOURCE = "flink-1.5.0-avro-1.8.2-evolution-serializer-snapshot";

	private static final String DATA_RESOURCE = "flink-1.5.0-avro-1.8.2-evolution-data";

	private static final String SNAPSHOT_OUTPUT_PATH = "/Users/twalthr/flink/tmp/snapshot/" + SNAPSHOT_RESOURCE;

	private static final String DATA_OUTPUT_PATH = "/Users/twalthr/flink/tmp/snapshot/" + DATA_RESOURCE;

	private static final Map<CharSequence, CharSequence> PROPERTIES = new HashMap<>();
	static {
		PROPERTIES.put("loves", "food");
		PROPERTIES.put("hates", "people");
	}

	/**
	 * Execute this method for creating a savepoint that contains a serialized {@link EvolutionUser}.
	 *
	 * <p>Note: We need to comment the 'age' field and uncomment the 'address' field in
	 * the 'evolution-user.avsc' Avro schema and regenerate the classes.
	 *
	 * <p>Uncomment the following lines after regeneration.
	 */
	public static void main(String[] args) throws IOException {
//	final EvolutionUser testData1 = EvolutionUser.newBuilder()
//		.setName("Bob Smith")
//		.setPriority(Priority.LOW)
//		.setProperties(PROPERTIES)
//		.setAddress(Address.newBuilder()
//			.setNum(42)
//			.setStreet("Some Street")
//			.setCity("Berlin")
//			.setState("Berlin")
//			.setZip("12345")
//			.build())
//		.build();
//
//	final EvolutionUser testData2 = EvolutionUser.newBuilder()
//		.setName("Alice Muller")
//		.setPriority(Priority.HIGH)
//		.setProperties(PROPERTIES)
//		.setAddress(Address.newBuilder()
//			.setNum(12)
//			.setStreet("5th Avenue")
//			.setCity("New York")
//			.setState("New York")
//			.setZip("45678")
//			.build())
//		.build();
//
//	final EvolutionUser testData3 = EvolutionUser.newBuilder()
//		.setName("Tyler Something")
//		.setPriority(Priority.HIGH)
//		.setProperties(Collections.emptyMap())
//		.setAddress(null)
//		.build();
//
//		final TypeInformation<EvolutionUser> typeInfo = new AvroTypeInfo<>(EvolutionUser.class);
//
//		final TypeSerializer<EvolutionUser> serializer = typeInfo.createSerializer(new ExecutionConfig());
//		final TypeSerializerConfigSnapshot configSnapshot = serializer.snapshotConfiguration();
//
//		try (FileOutputStream fos = new FileOutputStream(SNAPSHOT_OUTPUT_PATH)) {
//			final DataOutputView out = new DataOutputViewStreamWrapper(fos);
//
//			TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(
//				out,
//				Collections.singletonList(new Tuple2<>(serializer, configSnapshot)));
//		}
//
//		try (FileOutputStream fos = new FileOutputStream(DATA_OUTPUT_PATH)) {
//			final DataOutputView out = new DataOutputViewStreamWrapper(fos);
//
//			serializer.serialize(testData1, out);
//			serializer.serialize(testData2, out);
//			serializer.serialize(testData3, out);
//		}
	}

	@Test
	public void testCompatibilityWithFlink_1_5AndAvro_1_8_2() throws IOException {
		try (InputStream is = getClass().getClassLoader().getResourceAsStream(SNAPSHOT_RESOURCE)) {
			final DataInputView snapshotIn = new DataInputViewStreamWrapper(is);

			final List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> deserialized =
				TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(snapshotIn, getClass().getClassLoader());

			assertEquals(1, deserialized.size());

			final TypeSerializer<?> oldSerializer = deserialized.get(0).f0;
			final TypeSerializerConfigSnapshot oldConfigSnapshot = deserialized.get(0).f1;

			// test serializer and config snapshot
			assertNotNull(oldSerializer);
			assertNotNull(oldConfigSnapshot);
			assertTrue(oldSerializer instanceof AvroSerializer);
			assertTrue(oldConfigSnapshot instanceof AvroSerializer.AvroSchemaSerializerConfigSnapshot);
		}
	}
}
