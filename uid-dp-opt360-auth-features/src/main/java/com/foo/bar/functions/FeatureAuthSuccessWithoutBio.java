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
import java.util.LinkedList;
import java.util.Map;

public class FeatureAuthSuccessWithoutBio extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int WINDOW_SIZE = 30;
    private static final double ALERT_THRESHOLD = 0.40;

    public static class SuccessState {
        public LinkedList<Boolean> suspiciousSuccesses = new LinkedList<>();
        public int suspiciousCount = 0;
        public int totalSuccessCount = 0;
        public long lastTimestamp = 0;
        public boolean alerted = false;
    }

    private transient ValueState<SuccessState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthSuccessWithoutBioDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthSuccessWithoutBioDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        if (!"Y".equalsIgnoreCase(txn.getAuthResult())) return;

        SuccessState currentState = state.value();
        if (currentState == null) currentState = new SuccessState();

        int fmr = parseIntSafe(txn.getFmrCount());
        int fir = parseIntSafe(txn.getFirCount());
        boolean faceNo = "N".equalsIgnoreCase(txn.getFaceUsed()) || txn.getFaceUsed() == null;
        
        // A success is suspicious if it has NO biometric indicators
        boolean isSuspicious = (fmr == 0 && fir == 0 && faceNo);
        
        currentState.suspiciousSuccesses.addLast(isSuspicious);
        currentState.totalSuccessCount++;
        
        if (isSuspicious) currentState.suspiciousCount++;

        if (currentState.suspiciousSuccesses.size() > WINDOW_SIZE) {
            boolean removed = currentState.suspiciousSuccesses.removeFirst();
            if (removed) currentState.suspiciousCount--;
            currentState.totalSuccessCount--;
        }

        currentState.lastTimestamp = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        if (currentState.totalSuccessCount >= 10) {
            double ratio = (double) currentState.suspiciousCount / currentState.totalSuccessCount;
            
            if (ratio >= ALERT_THRESHOLD && !currentState.alerted) {
                emitFeature(ctx.getCurrentKey(), currentState, ratio, out);
                currentState.alerted = true;
            } else if (ratio < ALERT_THRESHOLD) {
                currentState.alerted = false;
            }
        }

        state.update(currentState);
    }

    private int parseIntSafe(String val) {
        if (val == null || val.isEmpty()) return 0;
        try {
            return Integer.parseInt(val);
        } catch (Exception e) {
            return 0;
        }
    }

    private void emitFeature(String key, SuccessState state, double ratio, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_success_without_bio_ratio_v1");
        feature.setFeatureType("Ratio");
        feature.setFeatureValue(ratio);
        feature.setWindowEnd(state.lastTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("suspicious_successes", state.suspiciousCount);
        comments.put("total_successes", state.totalSuccessCount);
        comments.put("suspicious_ratio", ratio);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
