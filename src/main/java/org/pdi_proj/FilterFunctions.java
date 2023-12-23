package org.pdi_proj;

import org.apache.flink.api.common.functions.FilterFunction;
import org.json.JSONObject;

public class FilterFunctions {
    public static final class AroundFiveKm implements FilterFunction<JSONObject> {
        @Override
        public boolean filter(JSONObject object) {
            final String distance = object
                    .getJSONObject("properties")
                    .getJSONObject("last_position")
                    .getString("shape_dist_traveled");

            final Double distance_dbl = Double.parseDouble(distance);


            return (distance_dbl >= 4.5) && (distance_dbl <= 5.5);
        }
    }
}
