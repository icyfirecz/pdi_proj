package org.pdi_proj;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.json.JSONObject;

import java.time.LocalDateTime;

public class TraveledFive1 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//      env.enableCheckpointing(60000);
        final DataStream<String> stream = env.socketTextStream("localhost", 12321);
        final DataStream<JSONObject> jsonized = stream.map(new MappingFunctions.Tokenizer()).name("Jsoning");
        final DataStream<Tuple4<String,String,Integer, LocalDateTime>> delays = jsonized.map(new MappingFunctions.JsonToTupleDelay());

        // 1. Assignment
        final DataStream<JSONObject> around5Km = jsonized.filter(new FilterFunctions.AroundFiveKm()).name("Around 5 km");
        around5Km.map(new MappingFunctions.Format5KmForPrint()).print();
        env.execute("1. Assignment");
    }
}
