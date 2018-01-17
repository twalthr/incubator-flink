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

package org.apache.flink.table.client.gateway;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.contrib.streaming.SocketStreamIterator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.net.InetAddress;

/**
 * Table sink for collecting the results locally.
 */
public class CollectTableSink implements RetractStreamTableSink<Row> {

	private final InetAddress gatewayAddress;
	private final int manualGatewayPort;

	private String[] fieldNames;
	private TypeInformation<?>[] fieldTypes;
	private SocketStreamIterator<Tuple2<Boolean, Row>> iterator;

	public CollectTableSink(InetAddress gatewayAddress, int manualGatewayPort) {
		this.gatewayAddress = gatewayAddress;
		this.manualGatewayPort = manualGatewayPort;
	}

	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		final CollectTableSink sink = new CollectTableSink(gatewayAddress, manualGatewayPort);
		sink.fieldNames = fieldNames;
		sink.fieldTypes = fieldTypes;
		return sink;
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return Types.ROW_NAMED(fieldNames, fieldTypes);
	}

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> stream) {

		// create socket stream iterator
		final TypeSerializer<Tuple2<Boolean, Row>> serializer = stream.getType().createSerializer(
				stream.getExecutionEnvironment().getConfig());


		dataStream.map(new MapFunction<Tuple2<Boolean, Row>, String>() {
			@Override
			public String map(Tuple2<Boolean, Row> value) throws Exception {
				Thread.sleep(100000);
				return null;
			}
		});
	}

	@Override
	public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
		return new TupleTypeInfo<>(Types.BOOLEAN, getRecordType());
	}
}
