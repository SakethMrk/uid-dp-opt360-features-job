package com.foo.bar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class FeatureAuthGeoDrift extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long WINDOW_MS = 60 * 60 * 1000L; // 1 hour

    public static class GeoState {
        public long windowStart = 0;
        public Set<String> states = new HashSet<>();
        public Set<String> districts = new HashSet<>();
        public boolean stateAlerted = false;
        public boolean districtAlerted = false;
    }

    private transient ValueState<GeoState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthGeoDriftDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthGeoDriftDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String stateCode = txn.getLocationStateCode();
        String districtCode = txn.getLocationDistrictCode();
        
        if (stateCode == null && districtCode == null) return;

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        GeoState currentState = state.value();
        if (currentState == null) {
            currentState = new GeoState();
            currentState.windowStart = currentTs;
        } else if (currentTs - currentState.windowStart > WINDOW_MS) {
            // Reset for new tumbling window manually
            currentState = new GeoState();
            currentState.windowStart = currentTs;
        }

        if (stateCode != null && !stateCode.trim().isEmpty()) {
            currentState.states.add(stateCode);
        }
        if (districtCode != null && !districtCode.trim().isEmpty()) {
            currentState.districts.add(districtCode);
        }

        if (currentState.states.size() > 2 && !currentState.stateAlerted) {
             emitFeature(ctx.getCurrentKey(), "auth_geo_states_drift_v1", currentState.states, currentTs, currentState.windowStart, out);
             currentState.stateAlerted = true;
        }

        if (currentState.districts.size() > 5 && !currentState.districtAlerted) {
             emitFeature(ctx.getCurrentKey(), "auth_geo_districts_drift_v1", currentState.districts, currentTs, currentState.windowStart, out);
             currentState.districtAlerted = true;
        }

        state.update(currentState);
    }

    private void emitFeature(String key, String featureName, Set<String> codes, long currentTs, long windowStart, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature(featureName);
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) codes.size());
        feature.setWindowStart(windowStart);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("distinct_count", codes.size());
        comments.put("codes_seen", new ArrayList<>(codes));

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
