package org.apache.flink.table.examples.java;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.Tumble;

public class StreamTableExample {

    public static void main(String[] args) throws Exception {
        List<Content> data = new ArrayList<Content>();
        data.add(new Content(1L, "Hi"));
        data.add(new Content(2L, "Hallo"));
        data.add(new Content(3L, "Hello"));
        data.add(new Content(4L, "Hello"));
        data.add(new Content(7L, "Hello"));
        data.add(new Content(8L, "Hello world"));
        data.add(new Content(16L, "Hello world"));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        DataStream<Content> stream = env.fromCollection(data);

        DataStream<Content> stream2 = stream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Content>(Time.milliseconds(1)) {

                    /**
                     *
                     */
                    private static final long serialVersionUID = 410512296011057717L;

                    @Override
                    public long extractTimestamp(Content element) {
                        return element.getRecordTime();
                    }

                });

        Table table = tableEnv.fromDataStream(stream2,
                "urlKey as urlKey," +
					"statusCodeCount as statusCodeCount," +
					"recordTime as rowtime.rowtime");
        table.window(Tumble.over("1.hours").on("rowtime").as("w")).groupBy("w, urlKey")
                .select("w.start,urlKey ");

        env.execute();
    }

    public static class Content implements Serializable {

        private String urlKey;

        private long recordTime;
        // private String recordTimeStr;

        private long httpGetMessageCount;
        private long httpPostMessageCount;
        private long uplink;
        private long downlink;
        private long statusCode;
        private long statusCodeCount;

        public Content() {
            super();
        }

        public Content(long recordTime, String urlKey) {
            super();
            this.recordTime = recordTime;
            this.urlKey = urlKey;
        }

        public String getUrlKey() {
            return urlKey;
        }

        public void setUrlKey(String urlKey) {
            this.urlKey = urlKey;
        }

        public long getRecordTime() {
            return recordTime;
        }

        public void setRecordTime(long recordTime) {
            this.recordTime = recordTime;
        }

        public long getHttpGetMessageCount() {
            return httpGetMessageCount;
        }

        public void setHttpGetMessageCount(long httpGetMessageCount) {
            this.httpGetMessageCount = httpGetMessageCount;
        }

        public long getHttpPostMessageCount() {
            return httpPostMessageCount;
        }

        public void setHttpPostMessageCount(long httpPostMessageCount) {
            this.httpPostMessageCount = httpPostMessageCount;
        }

        public long getUplink() {
            return uplink;
        }

        public void setUplink(long uplink) {
            this.uplink = uplink;
        }

        public long getDownlink() {
            return downlink;
        }

        public void setDownlink(long downlink) {
            this.downlink = downlink;
        }

        public long getStatusCode() {
            return statusCode;
        }

        public void setStatusCode(long statusCode) {
            this.statusCode = statusCode;
        }

        public long getStatusCodeCount() {
            return statusCodeCount;
        }

        public void setStatusCodeCount(long statusCodeCount) {
            this.statusCodeCount = statusCodeCount;
        }

    }

    private class TimestampWithEqualWatermark implements AssignerWithPunctuatedWatermarks<Object[]> {

        /**
         *
         */
        private static final long serialVersionUID = 1L;

        @Override
        public long extractTimestamp(Object[] element, long previousElementTimestamp) {
            // TODO Auto-generated method stub
            return (long) element[0];
        }

        @Override
        public Watermark checkAndGetNextWatermark(Object[] lastElement, long extractedTimestamp) {
            return new Watermark(extractedTimestamp);
        }

    }
}
