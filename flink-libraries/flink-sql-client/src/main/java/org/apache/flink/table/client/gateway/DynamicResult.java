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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A result of a dynamic table program.
 */
public class DynamicResult {

	private final CollectTableSink collectTableSink;
	private final List<Row> materializedTable;
	private final Map<Row, List<Integer>> rowPositions;
	private final ResultRetrievalThread retrievalThread;
	private final List<Row> snapshot;

	private int pageSize;

	// TODO start retrieval thread

	public DynamicResult(InetAddress targetAddress, int targetPort) {
		// create table sink
		collectTableSink = new CollectTableSink(targetAddress, targetPort);

		// start listener thread
		materializedTable = new ArrayList<>();
		rowPositions = new HashMap<>();
		retrievalThread = new ResultRetrievalThread();

		snapshot = new ArrayList<>();
	}

	public void open() {
		retrievalThread.start();
	}

	public int snapshot(int pageSize) {
		this.pageSize = pageSize;

		snapshot.clear();

		synchronized (materializedTable) {
			snapshot.addAll(materializedTable);
		}

		return snapshot.size() / pageSize;
	}

	public List<Row> retrieve(int page) {
		final int pageCount = (int) Math.ceil(((double) snapshot.size()) / pageSize);
		if (page < 0 || page > pageCount) {
			throw new SqlExecutionException("Invalid page '" + page + "'.");
		}

		return snapshot.subList(pageSize * page, Math.min(snapshot.size(), pageSize * (page + 1)));
	}

	public void cleanUp() {

	}

	public TableSink<?> getTableSink() {
		return collectTableSink;
	}

	public void close() {
		collectTableSink.close();
	}

	// --------------------------------------------------------------------------------------------

	private class ResultRetrievalThread extends Thread {

		@Override
		public void run() {
			while (collectTableSink.hasNext()) {
				final Tuple2<Boolean, Row> retraction = collectTableSink.next();
				final List<Integer> positions = rowPositions.get(retraction.f1);
				synchronized (materializedTable) {
					if (retraction.f0) {
						// insert
						materializedTable.add(retraction.f1);
						if (positions == null) {
							// new row
							final ArrayList<Integer> pos = new ArrayList<>(1);
							pos.add(materializedTable.size() - 1);
							rowPositions.put(retraction.f1, pos);
						} else {
							// row exists already
							positions.add(materializedTable.size() - 1);
							rowPositions.put(retraction.f1, positions);
						}
					} else {
						// delete
						if (positions != null) {
							// delete row position and row itself
							final int pos = positions.remove(0);
							materializedTable.remove(pos);
						}
					}
				}
			}
		}
	}
}
