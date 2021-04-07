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

package org.apache.flink.table.types.inference.transforms;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformation;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeInfoDataTypeConverter;

import javax.annotation.Nullable;

public class LegacyToNonLegacy implements TypeTransformation {

    public static final TypeTransformation INSTANCE = new LegacyToNonLegacy();

    @Override
    public DataType transform(DataType typeToTransform) {
        return transform(null, typeToTransform);
    }

    @Override
    public DataType transform(@Nullable DataTypeFactory factory, DataType dataType) {
        final LogicalType type = dataType.getLogicalType();
        if (type instanceof LegacyTypeInformationType) {
            return TypeInfoDataTypeConverter.toDataType(
                    new LazyDataTypeFactory(factory),
                    ((LegacyTypeInformationType<?>) type).getTypeInformation());
        }
        return dataType;
    }

    private static class LazyDataTypeFactory implements DataTypeFactory {

        private final @Nullable DataTypeFactory factory;

        LazyDataTypeFactory(@Nullable DataTypeFactory factory) {
            this.factory = factory;
        }

        private DataTypeFactory getFactoryOrThrow(Object unresolvedObject) {
            if (factory != null) {
                return factory;
            }
            throw new TableException(
                    String.format(
                            "Data type factory is not available at the current calling location for resolving: %s",
                            unresolvedObject));
        }

        @Override
        public DataType createDataType(AbstractDataType<?> abstractDataType) {
            return getFactoryOrThrow(abstractDataType).createDataType(abstractDataType);
        }

        @Override
        public DataType createDataType(String name) {
            return getFactoryOrThrow(name).createDataType(name);
        }

        @Override
        public DataType createDataType(UnresolvedIdentifier identifier) {
            return getFactoryOrThrow(identifier).createDataType(identifier);
        }

        @Override
        public <T> DataType createDataType(Class<T> clazz) {
            return getFactoryOrThrow(clazz).createDataType(clazz);
        }

        @Override
        public <T> DataType createDataType(TypeInformation<T> typeInfo) {
            return getFactoryOrThrow(typeInfo).createDataType(typeInfo);
        }

        @Override
        public <T> DataType createRawDataType(Class<T> clazz) {
            return getFactoryOrThrow(clazz).createRawDataType(clazz);
        }

        @Override
        public <T> DataType createRawDataType(TypeInformation<T> typeInfo) {
            return getFactoryOrThrow(typeInfo).createRawDataType(typeInfo);
        }
    }
}
