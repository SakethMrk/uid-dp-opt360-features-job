package com.foo.bar.functions.bio;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageBio;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

public class FeatureBioDeepPrintAnomaly extends KeyedProcessFunction<String, InputMessageBio, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int WINDOW_SIZE = 20;

    public static class DeepPrintState {
        public LinkedList<Boolean> recentResults = new LinkedList<>();
        public int failureCount = 0;
        public long lastTimestamp = 0;
        public boolean alerted = false;
    }

    private transient ValueState<DeepPrintState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureBioDeepPrintAnomalyDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureBioDeepPrintAnomalyDescriptor);
    }

    @Override
    public void processElement(InputMessageBio bio, Context ctx, Collector<OutMessage> out) throws Exception {
        String isMatched = bio.getDeepPrintIsMatched();
        if (isMatched == null || isMatched.trim().isEmpty()) return;

        boolean matched = "Y".equalsIgnoreCase(isMatched.trim());

        DeepPrintState currentState = state.value();
        if (currentState == null) currentState = new DeepPrintState();

        currentState.recentResults.addLast(matched);
        if (!matched) currentState.failureCount++;

        if (currentState.recentResults.size() > WINDOW_SIZE) {
            boolean removed = currentState.recentResults.removeFirst();
            if (!removed) currentState.failureCount--;
        }

        currentState.lastTimestamp = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        if (currentState.recentResults.size() >= WINDOW_SIZE) {
            double failureRate = (double) currentState.failureCount / WINDOW_SIZE;
            
            if (failureRate > 0.30 && !currentState.alerted) {
                emitFeature(ctx.getCurrentKey(), failureRate, currentState, out);
                currentState.alerted = true;
            } else if (failureRate <= 0.30) {
                currentState.alerted = false;
            }
        }

        state.update(currentState);
    }

    private void emitFeature(String key, double failureRate, DeepPrintState state, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("bio_deepprint_failure_rate_v1");
        feature.setFeatureType("Rate");
        feature.setFeatureValue(failureRate);
        feature.setWindowEnd(state.lastTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("failure_rate", failureRate);
        comments.put("failures", state.failureCount);
        comments.put("window_size", WINDOW_SIZE);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
