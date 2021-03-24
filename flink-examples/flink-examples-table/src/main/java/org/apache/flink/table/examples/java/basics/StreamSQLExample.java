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

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Objects;

/**
 * Simple example for demonstrating the use of SQL on a Stream Table in Java.
 *
 * <p>Usage: <code>StreamSQLExample --planner &lt;blink|flink&gt;</code><br>
 *
 * <p>This example shows how to: - Convert DataStreams to Tables - Register a Table under a name -
 * Run a StreamSQL query on the registered Table
 */
public class StreamSQLExample {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        String planner = params.has("planner") ? params.get("planner") : "blink";

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv;
        if (Objects.equals(planner, "blink")) { // use blink planner in streaming mode
            EnvironmentSettings settings =
                    EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else if (Objects.equals(planner, "flink")) { // use flink planner in streaming mode
            EnvironmentSettings settings =
                    EnvironmentSettings.newInstance().inStreamingMode().useOldPlanner().build();
            tEnv = StreamTableEnvironment.create(env, settings);
        } else {
            System.err.println(
                    "The planner is incorrect. Please run 'StreamSQLExample --planner <planner>', "
                            + "where planner (it is either flink or blink, and the default is blink) indicates whether the "
                            + "example uses flink planner or blink planner.");
            return;
        }

        SerializableTimestampAssigner<Integer> assigner = (element, recordTimestamp) -> element;

        DataStream<Integer> orderA =
                env.fromElements(1, 2, 3)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Integer>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(1))
                                        .withTimestampAssigner(assigner));

        // convert DataStream to Table
        Table t =
                tEnv.fromDataStream(
                        orderA,
                        Schema.newBuilder()
                                .columnByMetadata("rowtime", DataTypes.TIMESTAMP(3))
                                .watermark("rowtime", "source_watermark()")
                                .build());

        t.execute().print();

        //        System.out.println(
        //                tEnv.explainSql(
        //                        "SELECT f0, COUNT(*) FROM "
        //                                + t
        //                                + " GROUP BY f0, TUMBLE(rowtime, INTERVAL '0.001'
        // SECOND)"));
    }

    // *************************************************************************
    //     USER DATA TYPES
    // *************************************************************************

    /** Simple POJO. */
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        public Order() {}

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{"
                    + "user="
                    + user
                    + ", product='"
                    + product
                    + '\''
                    + ", amount="
                    + amount
                    + '}';
        }
    }
}
