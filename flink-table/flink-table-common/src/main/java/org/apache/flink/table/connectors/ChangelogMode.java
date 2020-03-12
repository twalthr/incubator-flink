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
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * The set of changes a {@link DynamicTableSource} produces or {@link DynamicTableSink} consumes.
 */
@PublicEvolving
public final class ChangelogMode {

	private final Set<RowKind> kinds;

	private ChangelogMode(Set<RowKind> kinds) {
		Preconditions.checkArgument(
			kinds.size() > 0,
			"At least one kind of row should be contained in a changelog.");
		this.kinds = Collections.unmodifiableSet(kinds);
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	public Set<RowKind> getContainedKinds() {
		return kinds;
	}

	public boolean contains(RowKind kind) {
		return kinds.contains(kind);
	}

	public boolean containsOnly(RowKind kind) {
		return kinds.size() == 1 && kinds.contains(kind);
	}

	// --------------------------------------------------------------------------------------------

	public static class Builder {

		private final Set<RowKind> kinds = EnumSet.noneOf(RowKind.class);

		public Builder() {
			// default constructor to allow a fluent definition
		}

		public Builder addContainedKind(RowKind kind) {
			this.kinds.add(kind);
			return this;
		}

		public ChangelogMode build() {
			return new ChangelogMode(kinds);
		}
	}
}
