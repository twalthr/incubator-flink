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

package org.apache.flink.table.client.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration of a table program execution.
 */
public class Execution {

	private final Map<String, String> properties;

	public Execution() {
		this.properties = Collections.emptyMap();
	}

	private Execution(Map<String, String> properties) {
		this.properties = properties;
	}

	// TODO add logger warnings if default value is used

	public boolean isStreamingExecution() {
		return Objects.equals(
			properties.getOrDefault(PropertyStrings.TYPE, PropertyStrings.EXECUTION_TYPE_STREAMING),
			PropertyStrings.EXECUTION_TYPE_STREAMING);
	}

	public boolean isBatchExecution() {
		return Objects.equals(
			properties.getOrDefault(PropertyStrings.TYPE, PropertyStrings.EXECUTION_TYPE_STREAMING),
			PropertyStrings.EXECUTION_TYPE_BATCH);
	}

	public long getMinStateRetention() {
		return Long.parseLong(properties.getOrDefault(PropertyStrings.EXECUTION_MIN_STATE_RETENTION, Long.toString(Long.MIN_VALUE)));
	}

	public long getMaxStateRetention() {
		return Long.parseLong(properties.getOrDefault(PropertyStrings.EXECUTION_MAX_STATE_RETENTION, Long.toString(Long.MIN_VALUE)));
	}

	public int getParallelism() {
		return Integer.parseInt(properties.getOrDefault(PropertyStrings.EXECUTION_PARALLELISM, Integer.toString(1)));
	}

	public int getMaxParallelism() {
		return Integer.parseInt(properties.getOrDefault(PropertyStrings.EXECUTION_MAX_PARALLELISM, Integer.toString(128)));
	}

	// --------------------------------------------------------------------------------------------

	public static Execution create(Map<String, Object> config) {
		return new Execution(ConfigUtil.normalizeYaml(config));
	}

	/**
	 * Merges two executions. The properties of the first execution might be overwritten by the second one.
	 */
	public static Execution merge(Execution exec1, Execution exec2) {
		final Map<String, String> properties = new HashMap<>(exec1.properties);
		properties.putAll(exec2.properties);

		return new Execution(properties);
	}
}
