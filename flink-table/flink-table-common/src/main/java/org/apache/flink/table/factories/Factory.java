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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;

import java.util.Optional;
import java.util.Set;

/**
 * A factory for creating instances from {@link ConfigOption}s in the table ecosystem. This
 * factory is used with Java's Service Provider Interfaces (SPI) for discovery.
 */
@PublicEvolving
public interface Factory {

	/**
	 * Uniquely identifies this factory when searching for a matching factory. Possibly versioned
	 * by {@link #factoryVersion()}.
	 */
	String factoryIdentifier();

	/**
	 * Extends a {@link #factoryIdentifier()} by a version when searching for a matching factory.
	 */
	Optional<String> factoryVersion();

	/**
	 * Definition of required options for this factory. The information will be used for generation
	 * of documentation and validation. It does not influence the discovery of a factory.
	 */
	Set<ConfigOption<?>> requiredOptions();

	/**
	 * Definition of optional options for this factory. This information will be used for generation
	 * of documentation and validation. It does not influence the discovery of a factory.
	 */
	Set<ConfigOption<?>> optionalOptions();
}
