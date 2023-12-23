package org.pdi_proj;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MappingFunctions {
    public static final class Tokenizer implements MapFunction<String, JSONObject> {
        @Override
        public JSONObject map(String object) {
            final JSONObject new_object = new JSONObject(object);
            return new_object;
        }
    }
    public static final class FormatOnlyTrams implements MapFunction<JSONObject, Tuple4<String,String,String, LocalDateTime>> {
        @Override
        public Tuple4<String,String,String,LocalDateTime> map(JSONObject object) {
            final String id = object.getJSONObject("properties")
                    .getJSONObject("trip")
                    .getJSONObject("gtfs")
                    .getString("trip_id");
            final String type = object
                    .getJSONObject("properties")
                    .getJSONObject("trip")
                    .getJSONObject("vehicle_type")
                    .getString("description_en");
            final String last_stop = object
                    .getJSONObject("properties")
                    .getJSONObject("last_position")
                    .getJSONObject("last_stop")
                    .getString("id");
            final String origin_timestamp = object
                    .getJSONObject("properties")
                    .getJSONObject("last_position")
                    .getString("origin_timestamp");
            DateTimeFormatter isoFormat = DateTimeFormatter.ISO_DATE_TIME;
            LocalDateTime dateTime = LocalDateTime.parse(origin_timestamp,isoFormat);
            String formatted_id;
            formatted_id = "Tram " + id;
            return new Tuple4<String,String,String,LocalDateTime>("2. "+formatted_id,"Stop:",last_stop,dateTime);
        }
    }
    public static final class JsonToTuple implements MapFunction<JSONObject, Tuple3<String,String,Double>> {
        @Override
        public Tuple3<String,String,Double> map(JSONObject obj) {
            final String id = obj.getJSONObject("properties")
                    .getJSONObject("trip")
                    .getJSONObject("gtfs")
                    .getString("trip_id");
            final Double delay = obj.getJSONObject("properties")
                    .getJSONObject("last_position")
                    .getJSONObject("delay")
                    .getDouble("actual");


            return new Tuple3<String,String,Double>("Key", id, delay);
        }
    }

    public static final class Format5KmForPrint implements MapFunction<JSONObject,Tuple3<String,String,Double>> {
        @Override
        public Tuple3<String,String,Double> map(JSONObject object) {
            final String id = object.getJSONObject("properties")
                    .getJSONObject("trip")
                    .getJSONObject("gtfs")
                    .getString("trip_id");
            final Double distance_traveled = Double.parseDouble(
                    object
                            .getJSONObject("properties")
                            .getJSONObject("last_position")
                            .getString("shape_dist_traveled")
            );

            return new Tuple3<String,String,Double>("1.ID:"+id,"traveled",distance_traveled);
        }
    }

    public static final class JsonToTupleDelay implements MapFunction<JSONObject, Tuple4<String,String,Integer,LocalDateTime>> {
        @Override
        public Tuple4<String,String,Integer,LocalDateTime> map(JSONObject object) {
            final String id = object.getJSONObject("properties")
                    .getJSONObject("trip")
                    .getJSONObject("gtfs")
                    .getString("trip_id");

            final Integer delay = object.getJSONObject("properties")
                    .getJSONObject("last_position")
                    .getJSONObject("delay")
                    .getInt("actual");

            final String origin_timestamp = object
                    .getJSONObject("properties")
                    .getJSONObject("last_position")
                    .getString("origin_timestamp");
            DateTimeFormatter isoFormat = DateTimeFormatter.ISO_DATE_TIME;
            LocalDateTime dateTime = LocalDateTime.parse(origin_timestamp,isoFormat);
            try{
                final String type = object
                        .getJSONObject("properties")
                        .getJSONObject("trip")
                        .getJSONObject("vehicle_type")
                        .getString("description_en");
                return new Tuple4<String,String,Integer,LocalDateTime>(id,type,delay,dateTime);
            } catch (Exception var4) {
                return new Tuple4<String,String,Integer,LocalDateTime>(id,"other",delay,dateTime);
            }


        }
    }
}
