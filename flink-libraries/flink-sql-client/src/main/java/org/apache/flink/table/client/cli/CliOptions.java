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

package org.apache.flink.table.client.cli;

import java.net.URL;

/**
 * Command line options to configure the SQL client.
 */
public class CliOptions {

	private final boolean isPrintHelp;
	private final URL config;
	private final String sessionId;
	private final URL jar;
	private final URL environment;
	private final URL defaults;
	private final URL libraries;

	public CliOptions(boolean isPrintHelp, URL config, String sessionId, URL jar, URL environment, URL defaults, URL libraries) {
		this.isPrintHelp = isPrintHelp;
		this.config = config;
		this.sessionId = sessionId;
		this.jar = jar;
		this.environment = environment;
		this.defaults = defaults;
		this.libraries = libraries;
	}

	public boolean isPrintHelp() {
		return isPrintHelp;
	}

	public URL getConfig() {
		return config;
	}

	public String getSessionId() {
		return sessionId;
	}

	public URL getJar() {
		return jar;
	}

	public URL getEnvironment() {
		return environment;
	}

	public URL getDefaults() {
		return defaults;
	}

	public URL getLibraries() {
		return libraries;
	}
}
