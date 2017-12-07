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
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A result of a dynamic table program.
 */
public class DynamicResult {

	private final boolean isChangelog;
	private final CollectTableSink collectTableSink;
	private final Object resultLock = new Object();
	private final ResultRetrievalThread retrievalThread;
	private final JobMonitoringThread monitoringThread;
	private Runnable program;
	private SqlExecutionException executionException;

	// for table materialization
	private final List<Row> materializedTable;
	private final Map<Row, List<Integer>> rowPositions; // positions of rows in table for faster access
	private final List<Row> snapshot;
	private int pageSize;
	private boolean isLastSnapshot;

	// for changelog
	private Tuple2<Boolean, Row> nextChangeRecord;

	public DynamicResult(InetAddress targetAddress, int targetPort, boolean isChangelog) {
		this.isChangelog = isChangelog;

		// create table sink
		collectTableSink = new CollectTableSink(targetAddress, targetPort);
		retrievalThread = new ResultRetrievalThread();
		monitoringThread = new JobMonitoringThread();

		// prepare for table materialization
		materializedTable = new ArrayList<>();
		rowPositions = new HashMap<>();
		snapshot = new ArrayList<>();
		isLastSnapshot = false;
	}

	public boolean isChangelog() {
		return isChangelog;
	}

	public void startRetrieval(Runnable program) {
		// start listener thread
		retrievalThread.start();

		// start program
		this.program = program;
		monitoringThread.start();
	}

	public TypedResult<Tuple2<Boolean, Row>> retrieveRecord() {
		synchronized (resultLock) {
			// retrieval thread is alive return a record if available
			// but the program must not have failed
			if (retrievalThread.isRunning && executionException == null) {
				if (nextChangeRecord == null) {
					return TypedResult.empty();
				} else {
					final Tuple2<Boolean, Row> change = nextChangeRecord;
					nextChangeRecord = null;
					resultLock.notify();
					return TypedResult.payload(change);
				}
			}
			// retrieval thread is dead but there is still a record to be delivered
			else if (!retrievalThread.isRunning && nextChangeRecord != null) {
				final Tuple2<Boolean, Row> change = nextChangeRecord;
				nextChangeRecord = null;
				return TypedResult.payload(change);
			}
			// no results can be returned anymore
			else {
				return handleMissingResult();
			}
		}
	}

	public TypedResult<Integer> snapshot(int pageSize) {
		synchronized (resultLock) {
			// retrieval thread is dead and there are no results anymore
			// or program failed
			if ((!retrievalThread.isRunning && isLastSnapshot) || executionException != null) {
				return handleMissingResult();
			}
			// this snapshot is the last result that can be delivered
			else if (!retrievalThread.isRunning) {
				isLastSnapshot = true;
			}

			this.pageSize = pageSize;
			snapshot.clear();
			snapshot.addAll(materializedTable);

			// at least one page
			final int pageCount = Math.max(1, (int) Math.ceil(((double) snapshot.size() / pageSize)));
			return TypedResult.payload(pageCount);
		}
	}

	public List<Row> retrievePage(int page) {
		final int pageCount = Math.max(1, (int) Math.ceil(((double) snapshot.size()) / pageSize));
		if (page <= 0 || page > pageCount) {
			throw new SqlExecutionException("Invalid page '" + page + "'.");
		}

		return snapshot.subList(pageSize * (page - 1), Math.min(snapshot.size(), pageSize * page));
	}

	public TableSink<?> getTableSink() {
		return collectTableSink;
	}

	public void close() {
		retrievalThread.isRunning = false;
		retrievalThread.interrupt();
		monitoringThread.interrupt();
		collectTableSink.close();
	}

	// --------------------------------------------------------------------------------------------

	private <T> TypedResult<T> handleMissingResult() {
		// check if the monitoring thread is still there
		// we need to wait until we know what is going on
		if (monitoringThread.isAlive()) {
			return TypedResult.empty();
		}
		// the job finished with an exception
		else if (executionException != null) {
			throw executionException;
		}
		// we assume that a bounded job finished
		else {
			return TypedResult.endOfStream();
		}
	}

	// --------------------------------------------------------------------------------------------

	private class JobMonitoringThread extends Thread {

		@Override
		public void run() {
			try {
				program.run();
			} catch (SqlExecutionException e) {
				executionException = e;
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	private class ResultRetrievalThread extends Thread {

		public volatile boolean isRunning = true;

		@Override
		public void run() {
			try {
				while (isRunning && collectTableSink.hasNext()) {
					final Tuple2<Boolean, Row> change = collectTableSink.next();

					if (isChangelog) {
						processForChangelog(change);
					} else {
						processForTable(change);
					}
				}
			} catch (RuntimeException e) {
				// ignore socket exceptions
			}

			// no result anymore
			// either the job is done or an error occurred
			isRunning = false;
		}

		private void processForChangelog(Tuple2<Boolean, Row> change) {
			synchronized (resultLock) {
				// wait until the last record has been picked up
				while (isRunning && nextChangeRecord != null) {
					try {
						resultLock.wait();
					} catch (InterruptedException e) {
						// ignore
					}
				}
				nextChangeRecord = change;
			}
		}

		private void processForTable(Tuple2<Boolean, Row> change) {
			// we track the position of rows for faster access and in order to return consistent
			// snapshots where new rows are appended at the end
			synchronized (resultLock) {
				final List<Integer> positions = rowPositions.get(change.f1);
				if (change.f0) {
					// insert
					materializedTable.add(change.f1);
					if (positions == null) {
						// new row
						final ArrayList<Integer> pos = new ArrayList<>(1);
						pos.add(materializedTable.size() - 1);
						rowPositions.put(change.f1, pos);
					} else {
						// row exists already
						positions.add(materializedTable.size() - 1);
						rowPositions.put(change.f1, positions);
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
