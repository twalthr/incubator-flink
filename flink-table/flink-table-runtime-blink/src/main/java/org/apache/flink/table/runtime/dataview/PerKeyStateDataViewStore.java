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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.conversion.DataStructureConverter;

/**
 * Default implementation of {@link StateDataViewStore} that currently forwards state registration
 * to a {@link RuntimeContext}.
 */
@Internal
public final class PerKeyStateDataViewStore implements StateDataViewStore {

	private static final String NULL_STATE_POSTFIX = "_null_state";

	private final RuntimeContext ctx;

	public PerKeyStateDataViewStore(RuntimeContext ctx) {
		this.ctx = ctx;
	}

	@Override
	public <N, IK, IV, EK, EV> StateMapView<N, EK, EV> getStateMapView(
			String stateName,
			boolean hasNullableKeys,
			DataStructureConverter<IK, EK> keyConverter,
			TypeSerializer<IK> keySerializer,
			DataStructureConverter<IV, EV> valueConverter,
			TypeSerializer<IV> valueSerializer) {

		final MapStateDescriptor<IK, IV> mapStateDescriptor = new MapStateDescriptor<>(
			stateName,
			keySerializer,
			valueSerializer);

		final MapState<IK, IV> mapState = ctx.getMapState(mapStateDescriptor);

		if (hasNullableKeys) {
			final ValueStateDescriptor<IV> nullStateDescriptor = new ValueStateDescriptor<>(
				stateName + NULL_STATE_POSTFIX,
				valueSerializer);

			final ValueState<IV> nullState = ctx.getState(nullStateDescriptor);

			return new StateMapView.KeyedStateMapViewWithKeysNullable<>(
				mapState,
				nullState,
				keyConverter,
				valueConverter);
		} else {
			return new StateMapView.KeyedStateMapViewWithKeysNotNull<>(
				mapState,
				keyConverter,
				valueConverter);
		}
	}

	@Override
	public <N, IT, ET> StateListView<N, ET> getStateListView(
			String stateName,
			DataStructureConverter<IT, ET> elementConverter,
			TypeSerializer<IT> elementSerializer) {

		final ListStateDescriptor<IT> listStateDescriptor = new ListStateDescriptor<>(
			stateName,
			elementSerializer);

		final ListState<IT> listState = ctx.getListState(listStateDescriptor);

		return new StateListView.KeyedStateListView<>(listState, elementConverter);
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		return ctx;
	}
}
