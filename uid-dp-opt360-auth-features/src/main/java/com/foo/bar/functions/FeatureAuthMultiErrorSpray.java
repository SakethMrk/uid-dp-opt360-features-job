package com.foo.bar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Feature: Error Probing Spray - tracking operators triggering 8+ distinct error codes 
 * within a 1-hour tumbling window.
 * 
 * Source Fields Used: Event ID, Sub Error Code, Request DateTime
 */
public class FeatureAuthMultiErrorSpray extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long WINDOW_MS = 60 * 60 * 1000L; // 1 hour
    private static final int SPRAY_THRESHOLD = 8;

    public static class SprayState {
        public long windowStart = 0;
        public Set<String> distinctErrors = new HashSet<>();
        public boolean alerted = false;
        public String lastProcessedEventId = "";
        public long lastTs = 0;
    }

    private transient ValueState<SprayState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthMultiErrorSprayDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthMultiErrorSprayDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String errCode = txn.getSubErrorCode();
        if (errCode == null || errCode.trim().isEmpty() || "null".equalsIgnoreCase(errCode)) return;

        SprayState currentState = state.value();
        if (currentState == null) currentState = new SprayState();

        String eventId = txn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) return;
            currentState.lastProcessedEventId = eventId;
        }

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();
        if (currentState.lastTs > 0 && currentTs <= currentState.lastTs) {
            currentTs = currentState.lastTs + 1;
        }
        currentState.lastTs = currentTs;

        if (currentState.windowStart == 0) {
            currentState.windowStart = currentTs;
        } else if (currentTs - currentState.windowStart > WINDOW_MS) {
            currentState.distinctErrors.clear();
            currentState.windowStart = currentTs;
            currentState.alerted = false;
        }

        currentState.distinctErrors.add(errCode);

        if (currentState.distinctErrors.size() >= SPRAY_THRESHOLD && !currentState.alerted) {
             emitFeature(ctx.getCurrentKey(), currentState.distinctErrors, currentState.windowStart, currentTs, out);
             currentState.alerted = true;
        }

        state.update(currentState);
    }

    private void emitFeature(String key, Set<String> errors, long startTs, long currentTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_multi_error_spray_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) errors.size());
        feature.setWindowStart(startTs);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("distinct_errors_1h", errors.size());
        comments.put("threshold", SPRAY_THRESHOLD);
        comments.put("error_codes", errors);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
