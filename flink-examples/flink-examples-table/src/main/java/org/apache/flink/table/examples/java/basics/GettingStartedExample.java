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

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.text.SimpleDateFormat;
import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.UNBOUNDED_RANGE;

public class GettingStartedExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<OrderStream> orderA =
                env.fromCollection(
                        Arrays.asList(
                                new OrderStream(
                                        1, 1L, "beer", 3, 1505529000L), // 2017-09-16 10:30:00
                                new OrderStream(
                                        2, 1L, "beer", 3, 1505529000L), // 2017-09-16 10:30:00
                                new OrderStream(
                                        3, 3L, "rubber", 2, 1505527800L), // 2017-09-16 10:10:00
                                new OrderStream(
                                        4, 3L, "rubber", 2, 1505527800L), // 2017-09-16 10:10:00
                                new OrderStream(
                                        5, 1L, "diaper", 4, 1505528400L), // 2017-09-16 10:20:00
                                new OrderStream(
                                        6, 1L, "diaper", 4, 1505528400L) // 2017-09-16 10:20:00
                                ));

        // -----------------------------------------------------------------------------------------------------
        Table orders =
                tEnv.fromDataStream(
                        orderA.assignTimestampsAndWatermarks(
                                new AssignerWithPeriodicWatermarks<OrderStream>() {

                                    Long currentMaxTimestamp = 0L;
                                    final Long maxOutOfOrderness = 10000L; // 最大允许的乱序时间是10s

                                    SimpleDateFormat sdf =
                                            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                                    //            @Nullable
                                    @Override
                                    public org.apache.flink.streaming.api.watermark.Watermark
                                            getCurrentWatermark() {

                                        return new Watermark(
                                                currentMaxTimestamp - maxOutOfOrderness);
                                    }

                                    @Override
                                    public long extractTimestamp(
                                            OrderStream element, long recordTimestamp) {
                                        long timestamp = element.rowtime;
                                        currentMaxTimestamp =
                                                Math.max(timestamp, currentMaxTimestamp);

                                        System.out.println(
                                                "id:"
                                                        + element.id
                                                        + ","
                                                        + "user:"
                                                        + element.user
                                                        + ","
                                                        + "eventtime:["
                                                        + element.rowtime
                                                        + "|"
                                                        + sdf.format(element.rowtime)
                                                        + ""
                                                        + "],currentMaxTimestamp:["
                                                        + currentMaxTimestamp
                                                        + "|"
                                                        + sdf.format(currentMaxTimestamp)
                                                        + "],watermark:["
                                                        + getCurrentWatermark().getTimestamp()
                                                        + "|"
                                                        + sdf.format(
                                                                getCurrentWatermark()
                                                                        .getTimestamp())
                                                        + "]");
                                        return timestamp;
                                    }
                                }),
                        $("user"),
                        $("product"),
                        $("amount"),
                        $("rowtime").rowtime());
        System.out.println(orders.getSchema());

        // -----------------------------------------------------------------------------------------------------

        // Distinct aggregation on over window
        Table result =
                orders.window(
                                Over.partitionBy($("user"))
                                        .orderBy($("rowtime"))
                                        .preceding(UNBOUNDED_RANGE)
                                        .as("w"))
                        .select(
                                $("user"),
                                $("amount").avg().distinct().over($("w")),
                                $("amount").max().over($("w")),
                                $("amount").min().over($("w")));

        tEnv.toRetractStream(result, Row.class).print();
        env.execute();
    }

    public static class OrderStream {
        public int id;
        public Long user;
        public String product;
        public int amount;
        public Long rowtime;

        public OrderStream() {}

        public OrderStream(int id, Long user, String product, int amount, Long rowtime) {
            this.id = id;
            this.user = user;
            this.product = product;
            this.amount = amount;
            this.rowtime = rowtime;
        }

        @Override
        public String toString() {
            return "Order{"
                    + "id="
                    + id
                    + ", user="
                    + user
                    + ", product='"
                    + product
                    + '\''
                    + ", amount="
                    + amount
                    + ", ts="
                    + rowtime
                    + '}';
        }
    }
}
