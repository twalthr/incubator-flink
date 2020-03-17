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
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;

/**
 * A {@link DynamicTableSource} that looks up rows of an external storage system by one or more
 * keys.
 */
@PublicEvolving
public interface LookupTableSource extends DynamicTableSource {

	/**
	 * Returns the actual implementation for reading the data.
	 */
	LookupRuntimeProvider getLookupRuntimeProvider(Context context);

	// --------------------------------------------------------------------------------------------
	// Helper Interfaces
	// --------------------------------------------------------------------------------------------

	interface Context {

		/**
		 * Returns the key fields that should be used during the lookup.
		 */
		List<FieldReferenceExpression> getKeyFields();

		/**
		 * Creates a runtime data structure converter that converts data of the given {@link DataType}
		 * to Flink's internal data structures.
		 *
		 * <p>Allows to implement runtime logic without depending on Flink's internal structures for
		 * timestamps, decimals, and structured types.
		 *
		 * @see LogicalType#supportsInputConversion(Class)
		 */
		DataStructureConverter createDataStructureConverter(DataType producedDataType);
	}

	interface LookupRuntimeProvider {
		// marker interface
	}
}
