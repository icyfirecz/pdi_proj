package org.pdi_proj;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.pdi_proj.MappingFunctions.*;
import org.pdi_proj.FilterFunctions.*;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 1. continously output vehicles that traveled 5 kms (+-0.5 km) [✓]
 * 2. list of trams (ID|name) with their last stop and time of the last actualization from the start of application [✓]
 * 3. list of 5 most delayed vehicles ordered by their delay []
 * 4. list of 5 most delayed vehicles in the last 3 minutes ordered by their time of actualization []
 * 5. print average delay of all vehicles actualized in the last 3 minutes []
 * 6. print out average distance traveled of last 10 actualized vehicles []
 */
public class DataProcessor {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//      env.enableCheckpointing(60000);
        final DataStream<String> stream = env.socketTextStream("localhost", 12321);
        final DataStream<JSONObject> jsonized = stream.map(new Tokenizer()).name("Jsoning");
        final DataStream<Tuple4<String,String,Integer,LocalDateTime>> delays = jsonized.map(new JsonToTupleDelay());

        // 1. Assignment
        final DataStream<JSONObject> around5Km = jsonized.filter(new AroundFiveKm()).name("Around 5 km");
        around5Km.map(new Format5KmForPrint()).print();


        // 2. Assignment
        final DataStream<JSONObject> onlyTrams = jsonized.filter(object -> {
            try{
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

        //final DataStream<Tuple3<String,String,Double>> tupled = jsonized.map(new JsonToTuple()).name("Tupling");
        // 3. Assignment


        //stream

        env.execute("Trying the streaming thing.");
    }
}
