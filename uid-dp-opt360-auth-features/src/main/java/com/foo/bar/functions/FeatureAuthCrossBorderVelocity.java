package com.foo.bar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Feature: Detects impossible geographical movement (changing State Codes within 2 hours).
 * 
 * Source Fields Used: Event ID, Location State Code
 */
public class FeatureAuthCrossBorderVelocity extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long IMPOSSIBLE_TRAVEL_MS = 2 * 60 * 60 * 1000L; // 2 hours

    public static class GeoVelocityState {
        public String lastStateCode = "";
        public long lastTs = 0;
        public String lastProcessedEventId = "";
    }

    private transient ValueState<GeoVelocityState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthCrossBorderVelocityDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthCrossBorderVelocityDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String stateCode = txn.getLocationStateCode();
        if (stateCode == null || stateCode.trim().isEmpty()) return;

        GeoVelocityState currentState = state.value();
        if (currentState == null) currentState = new GeoVelocityState();

        String eventId = txn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) return; // Dedup
            currentState.lastProcessedEventId = eventId;
        }

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();
        
        if (currentState.lastTs > 0 && currentTs <= currentState.lastTs) {
            currentTs = currentState.lastTs + 1;
        }

        if (!currentState.lastStateCode.isEmpty() && !currentState.lastStateCode.equals(stateCode)) {
            long travelTime = currentTs - currentState.lastTs;
            if (travelTime > 0 && travelTime < IMPOSSIBLE_TRAVEL_MS) {
                emitFeature(ctx.getCurrentKey(), currentState.lastStateCode, stateCode, travelTime, currentTs, currentState.lastTs, out);
            }
        }

        currentState.lastStateCode = stateCode;
        currentState.lastTs = currentTs;
        state.update(currentState);
    }

    private void emitFeature(String key, String fromState, String toState, long travelTimeMs, long currentTs, long lastTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_cross_border_velocity_v1");
        feature.setFeatureType("Duration (Mins)");
        feature.setFeatureValue((double) travelTimeMs / (60 * 1000L));
        feature.setWindowStart(lastTs);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("from_state", fromState);
        comments.put("to_state", toState);
        comments.put("travel_time_mins", travelTimeMs / (60 * 1000L));
        comments.put("threshold_mins", IMPOSSIBLE_TRAVEL_MS / (60 * 1000L));

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
