package com.foo.bar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class FeatureAuthErrorCodeHotspot extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long WINDOW_MS = 60 * 60 * 1000L; // 1 hour

    public static class ErrorState {
        public long windowStart = 0;
        public Map<String, Integer> errorCounts = new HashMap<>();
        public int totalErrors = 0;
        public boolean alerted = false;
    }

    private transient ValueState<ErrorState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthErrorCodeHotspotDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthErrorCodeHotspotDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String errCode = txn.getSubErrorCode();
        if (errCode == null || errCode.trim().isEmpty() || "null".equalsIgnoreCase(errCode)) return;

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        ErrorState currentState = state.value();
        if (currentState == null) {
            currentState = new ErrorState();
            currentState.windowStart = currentTs;
        } else if (currentTs - currentState.windowStart > WINDOW_MS) {
            currentState = new ErrorState();
            currentState.windowStart = currentTs;
        }

        currentState.errorCounts.put(errCode, currentState.errorCounts.getOrDefault(errCode, 0) + 1);
        currentState.totalErrors++;

        if (currentState.totalErrors >= 20) {
            int maxCount = 0;
            String dominantErr = "";
            for (Map.Entry<String, Integer> entry : currentState.errorCounts.entrySet()) {
                if (entry.getValue() > maxCount) {
                    maxCount = entry.getValue();
                    dominantErr = entry.getKey();
                }
            }

            double concentration = (double) maxCount / currentState.totalErrors;

            if (concentration > 0.80 && !currentState.alerted) {
                 emitFeature(ctx.getCurrentKey(), dominantErr, concentration, currentState, currentTs, out);
                 currentState.alerted = true;
            } else if (concentration <= 0.80) {
                 currentState.alerted = false;
            }
        }

        state.update(currentState);
    }

    private void emitFeature(String key, String err, double concentration, ErrorState state, long currentTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_error_code_hotspot_v1");
        feature.setFeatureType("Ratio");
        feature.setFeatureValue(concentration);
        feature.setWindowStart(state.windowStart);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("concentration_ratio", concentration);
        comments.put("dominant_error", err);
        comments.put("total_errors", state.totalErrors);
        comments.put("error_counts", state.errorCounts);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
