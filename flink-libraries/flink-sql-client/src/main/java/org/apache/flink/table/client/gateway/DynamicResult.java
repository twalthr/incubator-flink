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

import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.net.InetAddress;
import java.util.List;

/**
 * A result of a dynamic table program.
 */
public class DynamicResult {

	private final InetAddress targetAddress;
	private final int targetPort;
	private final CollectTableSink collectTableSink;

	// TODO start retrieval thread

	public DynamicResult(InetAddress targetAddress, int targetPort) {
		this.targetAddress = targetAddress;
		this.targetPort = targetPort;

		// create table sink
		collectTableSink = new CollectTableSink(targetAddress, targetPort);
	}

	public int snapshot(int pageSize) {
		return 0;
	}

	public List<Row> retrieve(int page) {
		return null;
	}

	public void cleanUp() {

	}

	public TableSink<?> getTableSink() {
		return collectTableSink;
	}

	public void close() {
		collectTableSink.close();
	}
}
