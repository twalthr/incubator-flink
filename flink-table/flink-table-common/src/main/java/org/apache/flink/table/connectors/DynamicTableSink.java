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

package org.apache.flink.table.connectors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

/**
 * Sink of a dynamic table to an external storage system.
 *
 * <p>A dynamic table sink can be seen as a factory that produces concrete runtime implementation.
 *
 * <p>Depending on optionally declared interfaces such as {@link SupportsPartitioning}, the planner
 * might apply changes to instances of this class and thus mutates the produced runtime
 * implementation.
 */
@PublicEvolving
public interface DynamicTableSink {

	/**
	 * Returns a string that summarizes this sink for printing to a console or log.
	 */
	String asSummaryString();

	/**
	 * Returns the {@link ChangelogMode} that this writer consumes.
	 *
	 * <p>The runtime can make suggestions but the sink has the final decision what it requires. If
	 * the runtime does not support this mode, it will throw an error. For example, the sink can
	 * return that it only supports {@link RowKind#INSERT}s.
	 *
	 * @param requestedMode expected kind of changes by the current plan
	 */
	ChangelogMode getChangelogMode(ChangelogMode requestedMode);

	/**
	 * Returns the actual implementation for writing the data.
	 */
	SinkRuntimeProvider getSinkRuntimeProvider(Context context);

	// --------------------------------------------------------------------------------------------
	// Helper Interfaces
	// --------------------------------------------------------------------------------------------

	interface Context {

		/**
		 * Creates a runtime data structure converter that converts Flink's internal data structures
		 * to data of the given {@link DataType}.
		 *
		 * <p>Allows to implement runtime logic without depending on Flink's internal structures for
		 * timestamps, decimals, and structured types.
		 *
		 * @see LogicalType#supportsOutputConversion(Class)
		 */
		DataStructureConverter createDataStructureConverter(DataType consumedDataType);
	}

	/**
	 * Converts data structures during runtime.
	 */
	interface DataStructureConverter extends RuntimeConverter {

		/**
		 * Converts the given object into an external data structure.
		 */
		@Nullable Object toExternal(@Nullable Object internalStructure);
	}

	interface SinkRuntimeProvider {
		// marker interface
	}
}
