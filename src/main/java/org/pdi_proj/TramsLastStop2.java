package org.pdi_proj;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.json.JSONObject;
import org.pdi_proj.FilterFunctions.*;
import org.pdi_proj.MappingFunctions.*;
import java.time.LocalDateTime;

public class TramsLastStop2 {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.enableCheckpointing(60000);
        final DataStream<String> stream = env.socketTextStream("localhost", 12321);
        final DataStream<JSONObject> jsonized = stream.map(new MappingFunctions.Tokenizer()).name("Jsoning");
        final DataStream<JSONObject> onlyTrams = jsonized.filter(object -> {
            try {
                final int type = object
                        .getJSONObject("properties")
                        .getJSONObject("trip")
                        .getJSONObject("vehicle_type")
                        .getInt("id");
                return type == 2;
            } catch (Exception var4) {
                return false;
            }
        });
        onlyTrams.map(new FormatOnlyTrams()).print();

        env.execute("2. Assignment");
    }
}
