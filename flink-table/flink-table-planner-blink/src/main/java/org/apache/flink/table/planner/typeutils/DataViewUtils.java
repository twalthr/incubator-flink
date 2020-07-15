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

package org.apache.flink.table.planner.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.dataview.DataView;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformation;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldNames;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasNested;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * Utilities to deal with {@link DataView}s.
 */
@Internal
public final class DataViewUtils {

	public static DataType newListView(DataType elementDataType) {
		return DataTypeUtils.newStructuredDataType(
			ListView.class,
			DataTypes.FIELD(
				"list",
				DataTypes.ARRAY(elementDataType).bridgedTo(List.class)));
	}

	public static DataType newMapView(DataType keyDataType, DataType valueDataType) {
		return DataTypeUtils.newStructuredDataType(
			MapView.class,
			DataTypes.FIELD(
				"map",
				DataTypes.MAP(keyDataType, valueDataType).bridgedTo(Map.class)));
	}

	/**
	 * Searches for data views in the data type of an accumulator and extracts them.
	 */
	public static List<DataViewSpec> extractDataViews(int aggIndex, DataType accumulatorDataType) {
		final LogicalType accumulatorType = accumulatorDataType.getLogicalType();
		if (!hasRoot(accumulatorType, LogicalTypeRoot.ROW) &&
				!hasRoot(accumulatorType, LogicalTypeRoot.STRUCTURED_TYPE)) {
			return Collections.emptyList();
		}
		final List<String> fieldNames = getFieldNames(accumulatorType);
		final List<DataType> fieldDataTypes = accumulatorDataType.getChildren();

		final List<DataViewSpec> specs = new ArrayList<>();
		for (int fieldIndex = 0; fieldIndex < fieldDataTypes.size(); fieldIndex++) {
			final DataType fieldDataType = fieldDataTypes.get(fieldIndex);
			final LogicalType fieldType = fieldDataType.getLogicalType();
			if (isDataView(fieldType, ListView.class)) {
				specs.add(
					new ListViewSpec(
						createStateId(aggIndex, fieldNames.get(fieldIndex)),
						fieldIndex,
						fieldDataType.getChildren().get(0))
				);
			} else if (isDataView(fieldType, MapView.class)) {
				specs.add(
					new MapViewSpec(
						createStateId(aggIndex, fieldNames.get(fieldIndex)),
						fieldIndex,
						fieldDataType.getChildren().get(0))
				);
			}
			if (fieldType.getChildren().stream().anyMatch(c -> hasNested(c, t -> isDataView(t, DataView.class)))) {
				throw new TableException(
					"Data views are only supported in the first level of a composite accumulator type.");
			}
		}
		return specs;
	}

	/**
	 * Adapts the data type of an accumulator regarding data views.
	 */
	public static DataType adaptDataViewsInDataType(boolean hasStateBackedDataViews, DataType accumulatorDataType) {
		if (!hasStateBackedDataViews) {
			return accumulatorDataType;
		}
		return DataTypeUtils.transform(accumulatorDataType, DataViewAsNullTransformation.INSTANCE);
	}

	private static boolean isDataView(LogicalType t, Class<? extends DataView> viewClass) {
		return hasRoot(t, LogicalTypeRoot.STRUCTURED_TYPE) &&
			((StructuredType) t).getImplementationClass().map(viewClass::isAssignableFrom).orElse(false);
	}

	private static String createStateId(int fieldIndex, String fieldName) {
		return "agg" + fieldIndex + "$" + fieldName;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Information about a {@link DataView}.
	 */
	public static abstract class DataViewSpec {

		private final String stateId;

		private final int fieldIndex;

		private DataViewSpec(String stateId, int fieldIndex) {
			this.stateId = stateId;
			this.fieldIndex = fieldIndex;
		}

		public String getStateId() {
			return stateId;
		}

		public int getFieldIndex() {
			return fieldIndex;
		}
	}

	/**
	 * Specification for {@link DataView}s that are exposed through the API in a composite type.
	 */
	public static abstract class ExternalDataViewSpec extends DataViewSpec {

		protected final DataType dataType;

		private ExternalDataViewSpec(String stateId, int fieldIndex, DataType dataType) {
			super(stateId, fieldIndex);
			this.dataType = dataType;
		}
	}

	/**
	 * Specification for a {@link ListView}.
	 */
	public static final class ListViewSpec extends ExternalDataViewSpec {

		public ListViewSpec(String stateId, int fieldIndex, DataType dataType) {
			super(stateId, fieldIndex, dataType);
		}

		public DataType getElementDataType() {
			return dataType.getChildren().get(0);
		}
	}

	/**
	 * Specification for a {@link MapView}.
	 */
	public static final class MapViewSpec extends ExternalDataViewSpec {

		public MapViewSpec(String stateId, int fieldIndex, DataType dataType) {
			super(stateId, fieldIndex, dataType);
		}

		public DataType getKeyDataType() {
			return dataType.getChildren().get(0);
		}

		public DataType getValueDataType() {
			return dataType.getChildren().get(1);
		}
	}

	/**
	 * Specification for a special {@link DataView} that makes distinct aggregates possible.
	 *
	 * <p>During runtime it behaves similar to {@link MapView} but works on internal data structures only.
	 */
	public static class DistinctViewSpec extends DataViewSpec {

		private final LogicalType keyType;

		private final LogicalType valueType;

		public DistinctViewSpec(String stateId, LogicalType keyType, LogicalType valueType) {
			super(stateId, -1);
			this.keyType = keyType;
			this.valueType = valueType;
		}

		public LogicalType getKeyType() {
			return keyType;
		}

		public LogicalType getValueType() {
			return valueType;
		}
	}

	// --------------------------------------------------------------------------------------------

	private static class DataViewAsNullTransformation implements TypeTransformation {

		private static final DataViewAsNullTransformation INSTANCE = new DataViewAsNullTransformation();

		@Override
		public DataType transform(DataType dataType) {
			if (isDataView(dataType.getLogicalType(), DataView.class)) {
				return DataTypes.NULL();
			}
			return dataType;
		}
	}

	private DataViewUtils() {
		// no instantiation
	}
}
