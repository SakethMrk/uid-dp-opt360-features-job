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

public class FeatureAuthFailureRate extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final double[] THRESHOLDS = {0.30, 0.50, 0.70, 0.90};
    private static final int WINDOW_SIZE = 50;

    public static class RateState {
        public LinkedList<Boolean> recentResults = new LinkedList<>();
        public int failureCount = 0;
        public double lastEmittedThreshold = 0.0;
        public long lastTimestamp = 0;
    }

    private transient ValueState<RateState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthFailureRateDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthFailureRateDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String result = txn.getAuthResult();
        if (result == null) return;

        RateState currentState = state.value();
        if (currentState == null) currentState = new RateState();

        boolean isFailure = "N".equalsIgnoreCase(result);
        
        currentState.recentResults.addLast(isFailure);
        if (isFailure) currentState.failureCount++;

        if (currentState.recentResults.size() > WINDOW_SIZE) {
            boolean removed = currentState.recentResults.removeFirst();
            if (removed) currentState.failureCount--;
        }

        currentState.lastTimestamp = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        if (currentState.recentResults.size() >= 20) { // arbitrary minimum sample size
            double currentRate = (double) currentState.failureCount / currentState.recentResults.size();
            double nextThreshold = getNextThreshold(currentState.lastEmittedThreshold);
            
            // Allow alert reset if rate drops significantly below the previous threshold
            if (currentState.lastEmittedThreshold > 0 && currentRate < currentState.lastEmittedThreshold - 0.2) {
                 currentState.lastEmittedThreshold = 0.0;
                 nextThreshold = THRESHOLDS[0];
            }

            if (currentRate >= nextThreshold && nextThreshold > 0) {
                emitFeature(ctx.getCurrentKey(), currentState, currentRate, out);
                currentState.lastEmittedThreshold = currentRate;
            }
        }

        state.update(currentState);
    }

    private double getNextThreshold(double currentLevel) {
        for (double threshold : THRESHOLDS) {
            if (currentLevel < threshold) return threshold;
        }
        return -1.0;
    }

    private void emitFeature(String key, RateState state, double rate, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_failure_rate_rolling_v1");
        feature.setFeatureType("Ratio");
        feature.setFeatureValue(rate);
        feature.setWindowEnd(state.lastTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("failure_count", state.failureCount);
        comments.put("total_events", state.recentResults.size());
        comments.put("failure_rate", rate);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
