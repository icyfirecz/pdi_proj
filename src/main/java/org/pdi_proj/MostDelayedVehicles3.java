package org.pdi_proj;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.Local;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * 3. list of 5 most delayed vehicles ordered by their delay []
 */
public class MostDelayedVehicles3 {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//      env.enableCheckpointing(60000);
        final DataStream<String> stream = env.socketTextStream("localhost", 12321);
        final DataStream<JSONObject> jsonized = stream.map(new MappingFunctions.Tokenizer()).name("Jsoning");
        final DataStream<Tuple4<String,String,Integer, LocalDateTime>> delays = jsonized.map(new MappingFunctions.JsonToTupleDelay());
        delays.process(new TopNProcessFunction(5))
                .print();
        env.execute("3. Assignment");
    }
    public static final class TopNProcessFunction extends ProcessFunction<Tuple4<String, String, Integer, LocalDateTime>, Tuple4<String, String, Integer, LocalDateTime>> {

        private final int n;
        private transient PriorityQueue<Tuple4<String, String, Integer, LocalDateTime>> topElements;

        public TopNProcessFunction(int n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.topElements = new PriorityQueue<>(n, Comparator.comparingInt(tuple -> tuple.f2));
        }

        @Override
        public void processElement(Tuple4<String, String, Integer, LocalDateTime> element, Context ctx, Collector<Tuple4<String, String, Integer, LocalDateTime>> out) throws Exception {
            topElements.offer(element);

            // Keep only the top N elements
            while (topElements.size() > n) {
                topElements.poll();
            }

            // Emit the top elements in order
            for (Tuple4<String, String, Integer, LocalDateTime> topElement : topElements) {
                out.collect(topElement);
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            topElements.clear();
        }
    }
}
