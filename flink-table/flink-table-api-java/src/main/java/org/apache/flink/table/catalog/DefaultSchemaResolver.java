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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;

/** Default implementation of {@link SchemaResolver}. */
@Internal
class DefaultSchemaResolver implements SchemaResolver {

    private final boolean isStreamingMode;
    private final boolean supportsMetadata;
    private final DataTypeFactory dataTypeFactory;
    private final ExpressionResolverFactory resolverFactory;

    DefaultSchemaResolver(boolean isStreamingMode, boolean supportsMetadata) {
        this.isStreamingMode = isStreamingMode;
        this.supportsMetadata = supportsMetadata;
    }

    public SchemaResolver withMetadata(boolean supportsMetadata) {
        return new DefaultSchemaResolver(isStreamingMode, supportsMetadata);
    }

    @Override
    public ResolvedSchema resolve(Schema schema) {
        return null;
    }

    @Override
    public boolean isStreamingMode() {
        return isStreamingMode;
    }

    @Override
    public boolean supportsMetadata() {
        return supportsMetadata;
    }
}
