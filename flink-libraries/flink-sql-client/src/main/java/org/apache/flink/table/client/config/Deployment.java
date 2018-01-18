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
 * Configuration of a Flink cluster deployment.
 */
public class Deployment {

	private final Map<String, String> properties;

	public Deployment() {
		this.properties = Collections.emptyMap();
	}

	private Deployment(Map<String, String> properties) {
		this.properties = properties;
	}

	public boolean isStandaloneDeployment() {
		return Objects.equals(
			properties.getOrDefault(PropertyStrings.TYPE, PropertyStrings.DEPLOYMENT_TYPE_STANDALONE),
			PropertyStrings.DEPLOYMENT_TYPE_STANDALONE);
	}

	public long getResponseTimeout() {
		return Long.parseLong(properties.getOrDefault(PropertyStrings.DEPLOYMENT_RESPONSE_TIMEOUT, Long.toString(10000)));
	}

	public String getGatewayAddress() {
		return properties.getOrDefault(PropertyStrings.DEPLOYMENT_GATEWAY_ADDRESS, "");
	}

	public int getGatewayPort() {
		return Integer.parseInt(properties.getOrDefault(PropertyStrings.DEPLOYMENT_GATEWAY_PORT, Integer.toString(0)));
	}

	// --------------------------------------------------------------------------------------------

	public static Deployment create(Map<String, Object> config) {
		return new Deployment(ConfigUtil.normalizeYaml(config));
	}

	/**
	 * Merges two deployments. The properties of the first deployment might be overwritten by the second one.
	 */
	public static Deployment merge(Deployment deploy1, Deployment deploy2) {
		final Map<String, String> properties = new HashMap<>(deploy1.properties);
		properties.putAll(deploy2.properties);

		return new Deployment(properties);
	}
}
