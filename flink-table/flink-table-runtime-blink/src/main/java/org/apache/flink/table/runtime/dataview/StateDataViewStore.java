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

package org.apache.flink.table.runtime.dataview;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.conversion.DataStructureConverter;

/**
 * This interface contains methods for registering {@link StateDataView} with a managed store.
 */
public interface StateDataViewStore {

	/**
	 * Creates a state map view.
	 *
	 * @param stateName The name of underlying state of the map view
	 * @param hasNullableKeys Whether a key can null
	 * @param keyConverter The key converter between internal and external structures
	 * @param keySerializer The key serializer of the map view
	 * @param valueConverter The value converter between internal and external structures
	 * @param valueSerializer The value serializer of the map view
	 * @param <N> Type of the namespace
	 * @param <IK> Internal type of the keys in the map state
	 * @param <IV> Internal type of the values in the map state
	 * @param <EK> External type of the keys in the map state
	 * @param <EV> External type of the values in the map state
	 * @return a keyed map state
	 */
	<N, IK, IV, EK, EV> StateMapView<N, EK, EV> getStateMapView(
		String stateName,
		boolean hasNullableKeys,
		DataStructureConverter<IK, EK> keyConverter,
		TypeSerializer<IK> keySerializer,
		DataStructureConverter<IV, EV> valueConverter,
		TypeSerializer<IV> valueSerializer) throws Exception;

	/**
	 * Creates a state list view.
	 *
	 * @param stateName The name of underlying state of the list view
	 * @param elementConverter The element converter between internal and external structures
	 * @param elementSerializer The element serializer of the list view
	 * @param <N> Type of the namespace
	 * @param <IT> Internal type of the elements in the list state
	 * @param <ET> External type of the elements in the list state
	 * @return a keyed list state
	 */
	<N, IT, ET> StateListView<N, ET> getStateListView(
		String stateName,
		DataStructureConverter<IT, ET> elementConverter,
		TypeSerializer<IT> elementSerializer) throws Exception;

	/**
	 * Gets the context that contains information about the UDF's runtime, such as the
	 * parallelism of the function, the subtask index of the function, or the name of
	 * the of the task that executes the function.
	 */
	RuntimeContext getRuntimeContext();
}
