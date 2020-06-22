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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.util.IterableIterator;

import javax.annotation.Nonnull;

import java.util.AbstractList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link ListView} which is backed by a state backend.
 *
 * @param <ET> the external element type
 */
@Internal
public abstract class StateListView<N, ET> extends ListView<ET> implements StateDataView<N> {

	private static final long serialVersionUID = 1L;

	private final Iterable<ET> emptyList = Collections.emptyList();

	protected abstract ListState<ET> getListState();

	@Override
	public Iterable<ET> get() throws Exception {
		Iterable<ET> original = getListState().get();
		return original != null ? original : emptyList;
	}

	@Override
	public void add(ET value) throws Exception {
		getListState().add(value);
	}

	@Override
	public void addAll(List<ET> list) throws Exception {
		getListState().addAll(list);
	}

	@Override
	public boolean remove(ET value) throws Exception {
		List<ET> list = (List<ET>) getListState().get();
		boolean success = list.remove(value);
		if (success) {
			getListState().update(list);
		}
		return success;
	}

	@Override
	public void clear() {
		getListState().clear();
	}

	/**
	 * A {@link MapView} backed by a keyed state.
	 */
	public static final class KeyedStateListView<N, IT, ET> extends StateListView<N, ET> {

		private static final long serialVersionUID = 6526065473887440980L;

		private final ListState<ET> externalListState;

		public KeyedStateListView(
				ListState<IT> internalListState,
				DataStructureConverter<IT, ET> elementConverter) {
			this.externalListState = new ExternalListState<>(internalListState, elementConverter);
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected ListState<ET> getListState() {
			return externalListState;
		}
	}

	/**
	 * A {@link MapView} backed by a keyed state with namespace.
	 */
	public static final class NamespacedStateListView<N, IT, ET> extends StateListView<N, ET> {

		private static final long serialVersionUID = 1423184510190367940L;

		private final InternalListState<?, N, IT> internalListState;
		private final ListState<ET> externalListState;

		private N namespace;

		public NamespacedStateListView(
				InternalListState<?, N, IT> internalListState,
				DataStructureConverter<IT, ET> elementConverter) {
			this.internalListState = internalListState;
			this.externalListState = new ExternalListState<>(internalListState, elementConverter);
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			this.namespace = namespace;
		}

		@Override
		protected ListState<ET> getListState() {
			internalListState.setCurrentNamespace(namespace);
			return externalListState;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Helper classes
	// --------------------------------------------------------------------------------------------

	private static final class ExternalListState<IT, ET> implements ListState<ET> {

		private final ListState<IT> internalListState;
		private final DataStructureConverter<IT, ET> elementConverter;
		private final InternalHelperList helperList = new InternalHelperList();

		private ExternalListState(
				ListState<IT> internalListState,
				DataStructureConverter<IT, ET> elementConverter) {
			this.internalListState = internalListState;
			this.elementConverter = elementConverter;
		}

		@Override
		public void update(List<ET> externalValues) throws Exception {
			if (externalValues == null) {
				// let the state backend throw an exception if necessary
				internalListState.update(null);
			} else {
				helperList.externalValues = externalValues;
				internalListState.update(helperList);
			}
		}

		@Override
		public void addAll(List<ET> externalValues) throws Exception {
			if (externalValues == null) {
				// let the state backend throw an exception if necessary
				internalListState.addAll(null);
			} else {
				helperList.externalValues = externalValues;
				internalListState.addAll(helperList);
			}
		}

		@Override
		public Iterable<ET> get() throws Exception {
			final Iterable<IT> internalIterable = internalListState.get();
			if (internalIterable == null) {
				return null;
			}
			return new ElementsIterator(internalIterable.iterator());
		}

		@Override
		public void add(ET externalValue) throws Exception {
			final IT internalValue = elementConverter.toInternalOrNull(externalValue);
			internalListState.add(internalValue);
		}

		@Override
		public void clear() {
			internalListState.clear();
		}

		private final class InternalHelperList extends AbstractList<IT> {

			public List<ET> externalValues = null;

			@Override
			public IT get(int index) {
				final ET externalValue = externalValues.get(index);
				return elementConverter.toInternalOrNull(externalValue);
			}

			@Override
			public int size() {
				return externalValues.size();
			}
		}

		private final class ElementsIterator implements IterableIterator<ET> {

			private final Iterator<IT> internalIterator;

			private ElementsIterator(Iterator<IT> internalIterator) {
				this.internalIterator = internalIterator;
			}

			@Override
			@Nonnull
			public Iterator<ET> iterator() {
				return this;
			}

			@Override
			public boolean hasNext() {
				return internalIterator.hasNext();
			}

			@Override
			public ET next() {
				return elementConverter.toExternalOrNull(internalIterator.next());
			}
		}
	}
}
