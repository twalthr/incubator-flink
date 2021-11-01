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

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/** The famous word count example that shows a minimal Flink SQL job in batch execution mode. */
public final class WordCountSQLExample {

    public static void main(String[] args) throws Exception {

        ObjectMapper m = new ObjectMapper();

        m.readValue(bytes, MyPojo.class)

        DataType dt =
                DataTypes.STRUCTURED(
                        User.class,
                        DataTypes.FIELD("i", DataTypes.INT().bridgedTo(int.class)),
                        DataTypes.FIELD("s", DataTypes.STRING().bridgedTo(String.class)));

        DataStructureConverter<Object, Object> converter = DataStructureConverters.getConverter(dt);

        RowData rowData = (RowData) converter.toInternal(new User());

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnv.fromValues(1, 2, 3).execute().collect().forEachRemaining(System.out::println);
    }

    public static class User {
        int i;

        String s;
    }
}
