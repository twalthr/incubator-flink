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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.util.IterableIterator;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * A {@link MapView} which is backed by a state backend.
 *
 * @param <N> the type of namespace
 * @param <EK> the external type of the {@link MapView} key
 * @param <EV> the external type of the {@link MapView} value
 */
@Internal
public abstract class StateMapView<N, EK, EV> extends MapView<EK, EV> implements StateDataView<N> {

	private static final long serialVersionUID = 1L;

	/**
	 * A {@link MapView} backed by a state backend were keys must not be null.
	 *
	 * <p>This is the default implementation for {@link StateMapView}.
	 *
	 * @param <N> the type of namespace
	 * @param <EK> the external key type
	 * @param <EV> the external value type
	 */
	private abstract static class StateMapViewWithKeysNotNull<N, EK, EV>
			extends StateMapView<N, EK, EV> {

		private static final long serialVersionUID = 2605280027745112384L;

		private final Map<EK, EV> emptyState = Collections.emptyMap();

		protected abstract MapState<EK, EV> getMapState();

		@Override
		public EV get(EK key) throws Exception {
			return getMapState().get(key);
		}

		@Override
		public void put(EK key, EV value) throws Exception {
			getMapState().put(key, value);
		}

		@Override
		public void putAll(Map<EK, EV> map) throws Exception {
			getMapState().putAll(map);
		}

		@Override
		public void remove(EK key) throws Exception {
			getMapState().remove(key);
		}

		@Override
		public boolean contains(EK key) throws Exception {
			return getMapState().contains(key);
		}

		@Override
		public Iterable<Map.Entry<EK, EV>> entries() throws Exception {
			Iterable<Map.Entry<EK, EV>> original = getMapState().entries();
			return original != null ? original : emptyState.entrySet();
		}

		@Override
		public Iterable<EK> keys() throws Exception {
			Iterable<EK> original = getMapState().keys();
			return original != null ? original : emptyState.keySet();
		}

		@Override
		public Iterable<EV> values() throws Exception {
			Iterable<EV> original = getMapState().values();
			return original != null ? original : emptyState.values();
		}

		@Override
		public Iterator<Map.Entry<EK, EV>> iterator() throws Exception {
			Iterator<Map.Entry<EK, EV>> original = getMapState().iterator();
			return original != null ? original : emptyState.entrySet().iterator();
		}

		@Override
		public boolean isEmpty() throws Exception {
			return getMapState().isEmpty();
		}

		@Override
		public void clear() {
			getMapState().clear();
		}
	}

	/**
	 * A {@link MapView} backed by a state backend were keys can be null.
	 *
	 * <p>This is only used internally when implementing distinct aggregates.
	 *
	 * @param <N> the type of namespace
	 * @param <EK> the external key type
	 * @param <EV> the external value type
	 */
	private abstract static class StateMapViewWithKeysNullable<N, EK, EV>
			extends StateMapView<N, EK, EV> {

		private static final long serialVersionUID = 2605280027745112384L;

		@Override
		public EV get(EK key) throws Exception {
			if (key == null) {
				return getNullState().value();
			} else {
				return getMapState().get(key);
			}
		}

		@Override
		public void put(EK key, EV value) throws Exception {
			if (key == null) {
				getNullState().update(value);
			} else {
				getMapState().put(key, value);
			}
		}

		@Override
		public void putAll(Map<EK, EV> map) throws Exception {
			for (Map.Entry<EK, EV> entry : map.entrySet()) {
				// entry key might be null, so we can't invoke mapState.putAll(map) directly here
				put(entry.getKey(), entry.getValue());
			}
		}

		@Override
		public void remove(EK key) throws Exception {
			if (key == null) {
				getNullState().clear();
			} else {
				getMapState().remove(key);
			}
		}

		@Override
		public boolean contains(EK key) throws Exception {
			if (key == null) {
				return getNullState().value() != null;
			} else {
				return getMapState().contains(key);
			}
		}

		@Override
		public Iterable<Map.Entry<EK, EV>> entries() throws Exception {
			final Iterator<Map.Entry<EK, EV>> iterator = iterator();
			return () -> iterator;
		}

		@Override
		public Iterable<EK> keys() throws Exception {
			return new KeysIterable<>(this.iterator());
		}

		@Override
		public Iterable<EV> values() throws Exception {
			return new ValuesIterable<>(this.iterator());
		}

		@Override
		public Iterator<Map.Entry<EK, EV>> iterator() throws Exception {
			return new NullAwareMapIterator<>(getMapState().iterator(), new NullMapEntryImpl());
		}

		@Override
		public boolean isEmpty() throws Exception {
			return getMapState().isEmpty() && getNullState().value() == null;
		}

		@Override
		public void clear() {
			getMapState().clear();
			getNullState().clear();
		}

		protected abstract MapState<EK, EV> getMapState();

		protected abstract ValueState<EV> getNullState();

		/**
		 * {@link Map.Entry} for the null key of this {@link MapView}.
		 */
		private final class NullMapEntryImpl implements NullAwareMapIterator.NullMapEntry<EK, EV> {

			@Override
			public EV getValue() {
				try {
					return getNullState().value();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public EV setValue(EV value) {
				final EV oldValue;
				try {
					oldValue = getNullState().value();
					getNullState().update(value);
					return oldValue;
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void remove() {
				getNullState().clear();
			}
		}
	}

	/**
	 * A {@link MapView} backed by a state backend which supports neither a namespace nor nullability in keys.
	 */
	public static final class KeyedStateMapViewWithKeysNotNull<N, IK, IV, EK, EV>
			extends StateMapViewWithKeysNotNull<N, EK, EV> {

		private static final long serialVersionUID = 6650061094951931356L;

		private final MapState<EK, EV> externalMapState;

		public KeyedStateMapViewWithKeysNotNull(
				MapState<IK, IV> internalMapState,
				DataStructureConverter<IK, EK> keyConverter,
				DataStructureConverter<IV, EV> valueConverter) {
			this.externalMapState = new ExternalMapState<>(internalMapState, keyConverter, valueConverter);
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected MapState<EK, EV> getMapState() {
			return externalMapState;
		}
	}

	/**
	 * A {@link MapView} backed by a state backend which supports a namespace but no nullability in keys.
	 */
	public static final class NamespacedStateMapViewWithKeysNotNull<N, IK, IV, EK, EV>
			extends StateMapViewWithKeysNotNull<N, EK, EV> {

		private static final long serialVersionUID = -2793150592169689571L;

		private final InternalMapState<?, N, IK, IV> internalMapState;
		private final MapState<EK, EV> externalMapState;

		private N namespace;

		public NamespacedStateMapViewWithKeysNotNull(
				InternalMapState<?, N, IK, IV> internalMapState,
				DataStructureConverter<IK, EK> keyConverter,
				DataStructureConverter<IV, EV> valueConverter) {
			this.internalMapState = internalMapState;
			this.externalMapState = new ExternalMapState<>(internalMapState, keyConverter, valueConverter);
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			this.namespace = namespace;
		}

		@Override
		protected MapState<EK, EV> getMapState() {
			internalMapState.setCurrentNamespace(namespace);
			return externalMapState;
		}
	}

	/**
	 * A {@link MapView} backed by a state backend which supports nullability in keys but no namespace.
	 */
	public static final class KeyedStateMapViewWithKeysNullable<N, IK, IV, EK, EV>
			extends StateMapViewWithKeysNullable<N, EK, EV> {

		private static final long serialVersionUID = -4222930534937318207L;

		private final MapState<EK, EV> externalMapState;
		private final ValueState<EV> externalNullState;

		public KeyedStateMapViewWithKeysNullable(
				MapState<IK, IV> internalMapState,
				ValueState<IV> internalNullState,
				DataStructureConverter<IK, EK> keyConverter,
				DataStructureConverter<IV, EV> valueConverter) {
			this.externalMapState = new ExternalMapState<>(internalMapState, keyConverter, valueConverter);
			this.externalNullState = new ExternalValueState<>(internalNullState, valueConverter);
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected MapState<EK, EV> getMapState() {
			return externalMapState;
		}

		@Override
		protected ValueState<EV> getNullState() {
			return externalNullState;
		}
	}

	/**
	 * A {@link MapView} backed by a state backend which supports nullability in keys and a namespace.
	 */
	public static final class NamespacedStateMapViewWithKeysNullable<N, IK, IV, EK, EV>
			extends StateMapViewWithKeysNullable<N, EK, EV> {

		private static final long serialVersionUID = -6915428707804508152L;

		private final InternalMapState<?, N, IK, IV> internalMapState;
		private final InternalValueState<?, N, IV> internalNullState;
		private final MapState<EK, EV> externalMapState;
		private final ValueState<EV> externalNullState;

		private N namespace;

		public NamespacedStateMapViewWithKeysNullable(
				InternalMapState<?, N, IK, IV> internalMapState,
				InternalValueState<?, N, IV> internalNullState,
				DataStructureConverter<IK, EK> keyConverter,
				DataStructureConverter<IV, EV> valueConverter) {
			this.internalMapState = internalMapState;
			this.internalNullState = internalNullState;
			this.externalMapState = new ExternalMapState<>(internalMapState, keyConverter, valueConverter);
			this.externalNullState = new ExternalValueState<>(internalNullState, valueConverter);
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			this.namespace = namespace;
		}

		@Override
		protected MapState<EK, EV> getMapState() {
			internalMapState.setCurrentNamespace(namespace);
			return externalMapState;
		}

		@Override
		protected ValueState<EV> getNullState() {
			internalNullState.setCurrentNamespace(namespace);
			return externalNullState;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Helper classes
	// --------------------------------------------------------------------------------------------

	private static final class ExternalValueState<IV, EV> implements ValueState<EV> {

		private final ValueState<IV> internalValueState;
		private final DataStructureConverter<IV, EV> valueConverter;

		private ExternalValueState(
				ValueState<IV> internalValueState,
				DataStructureConverter<IV, EV> valueConverter) {
			this.internalValueState = internalValueState;
			this.valueConverter = valueConverter;
		}

		@Override
		public EV value() throws IOException {
			return valueConverter.toExternalOrNull(internalValueState.value());
		}

		@Override
		public void update(EV value) throws IOException {
			final IV internalValue = valueConverter.toInternalOrNull(value);
			internalValueState.update(internalValue);
		}

		@Override
		public void clear() {
			internalValueState.clear();
		}
	}

	private static final class ExternalMapState<IK, IV, EK, EV> implements MapState<EK, EV> {

		private final MapState<IK, IV> internalMapState;
		private final DataStructureConverter<IK, EK> keyConverter;
		private final DataStructureConverter<IV, EV> valueConverter;

		private ExternalMapState(
				MapState<IK, IV> internalMapState,
				DataStructureConverter<IK, EK> keyConverter,
				DataStructureConverter<IV, EV> valueConverter) {
			this.internalMapState = internalMapState;
			this.keyConverter = keyConverter;
			this.valueConverter = valueConverter;
		}

		@Override
		public EV get(EK externalKey) throws Exception {
			final IK internalKey = keyConverter.toInternalOrNull(externalKey);
			final IV internalValue = internalMapState.get(internalKey);
			return valueConverter.toExternalOrNull(internalValue);
		}

		@Override
		public void put(EK externalKey, EV externalValue) throws Exception {
			final IK internalKey = keyConverter.toInternalOrNull(externalKey);
			final IV internalValue = valueConverter.toInternalOrNull(externalValue);
			internalMapState.put(internalKey, internalValue);
		}

		@Override
		public void putAll(Map<EK, EV> externalMap) throws Exception {
			for (Map.Entry<EK, EV> entry : externalMap.entrySet()) {
				put(entry.getKey(), entry.getValue());
			}
		}

		@Override
		public void remove(EK externalKey) throws Exception {
			final IK internalKey = keyConverter.toInternalOrNull(externalKey);
			internalMapState.remove(internalKey);
		}

		@Override
		public boolean contains(EK externalKey) throws Exception {
			final IK internalKey = keyConverter.toInternalOrNull(externalKey);
			return internalMapState.contains(internalKey);
		}

		@Override
		public Iterable<Map.Entry<EK, EV>> entries() throws Exception {
			final Iterator<Map.Entry<EK, EV>> iterator = iterator();
			return () -> iterator;
		}

		@Override
		public Iterable<EK> keys() throws Exception {
			return new KeysIterable<>(iterator());
		}

		@Override
		public Iterable<EV> values() throws Exception {
			return new ValuesIterable<>(iterator());
		}

		@Override
		public Iterator<Map.Entry<EK, EV>> iterator() throws Exception {
			return new ExternalEntryIterator(internalMapState.iterator());
		}

		@Override
		public boolean isEmpty() throws Exception {
			return internalMapState.isEmpty();
		}

		@Override
		public void clear() {
			internalMapState.clear();
		}

		private final class ExternalEntryIterator implements Iterator<Map.Entry<EK, EV>> {

			private final Iterator<Map.Entry<IK, IV>> internalIterator;

			private ExternalEntryIterator(Iterator<Map.Entry<IK, IV>> internalIterator) {
				this.internalIterator = internalIterator;
			}

			@Override
			public boolean hasNext() {
				return internalIterator.hasNext();
			}

			@Override
			public Map.Entry<EK, EV> next() {
				final Map.Entry<IK, IV> internalEntry = internalIterator.next();
				return new ExternalMapEntry(internalEntry);
			}

			@Override
			public void remove() {
				internalIterator.remove();
			}
		}

		private final class ExternalMapEntry implements Map.Entry<EK, EV> {

			private final Map.Entry<IK, IV> internalEntry;

			private ExternalMapEntry(Map.Entry<IK, IV> internalEntry) {
				this.internalEntry = internalEntry;
			}

			@Override
			public EK getKey() {
				return keyConverter.toExternalOrNull(internalEntry.getKey());
			}

			@Override
			public EV getValue() {
				return valueConverter.toExternalOrNull(internalEntry.getValue());
			}

			@Override
			public EV setValue(EV externalNewValue) {
				final IV internalNewValue = valueConverter.toInternalOrNull(externalNewValue);
				final IV internalOldValue = internalEntry.setValue(internalNewValue);
				return valueConverter.toExternalOrNull(internalOldValue);
			}
		}
	}

	/**
	 * {@link Iterable} over the keys of this {@link MapView}.
	 */
	private static final class KeysIterable<EK, EV> implements IterableIterator<EK> {

		private final Iterator<Map.Entry<EK, EV>> iterator;

		private KeysIterable(Iterator<Map.Entry<EK, EV>> iterator) {
			this.iterator = iterator;
		}

		@Override
		@Nonnull
		public Iterator<EK> iterator() {
			return this;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public EK next() {
			return iterator.next().getKey();
		}
	}

	/**
	 * {@link Iterable} over the values of this {@link MapView}.
	 */
	private static final class ValuesIterable<EK, EV> implements IterableIterator<EV> {

		private final Iterator<Map.Entry<EK, EV>> iterator;

		private ValuesIterable(Iterator<Map.Entry<EK, EV>> iterator) {
			this.iterator = iterator;
		}

		@Override
		@Nonnull
		public Iterator<EV> iterator() {
			return this;
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public EV next() {
			return iterator.next().getValue();
		}
	}
}
