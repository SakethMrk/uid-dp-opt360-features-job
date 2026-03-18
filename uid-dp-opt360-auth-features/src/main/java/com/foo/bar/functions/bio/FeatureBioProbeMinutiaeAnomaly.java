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
import java.util.Map;

public class FeatureBioProbeMinutiaeAnomaly extends KeyedProcessFunction<String, InputMessageBio, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final double STDDEV_OVER_ALLOWANCE = 15.0;

    public static class MinutiaeState {
        public long count = 0;
        public double mean = 0.0;
        public double m2 = 0.0;
        public long lastTimestamp = 0;
    }

    private transient ValueState<MinutiaeState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureBioProbeMinutiaeAnomalyDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureBioProbeMinutiaeAnomalyDescriptor);
    }

    @Override
    public void processElement(InputMessageBio bio, Context ctx, Collector<OutMessage> out) throws Exception {
        String minutiaeStr = bio.getProbeMinutiae();
        if (minutiaeStr == null || minutiaeStr.trim().isEmpty() || "null".equalsIgnoreCase(minutiaeStr)) return;

        double minutiaeCount;
        try {
            minutiaeCount = Double.parseDouble(minutiaeStr.trim());
        } catch (NumberFormatException e) {
            return;
        }

        if (minutiaeCount <= 0) return;

        MinutiaeState currentState = state.value();
        if (currentState == null) currentState = new MinutiaeState();

        // Welford's online algorithm
        currentState.count++;
        double delta = minutiaeCount - currentState.mean;
        currentState.mean += delta / currentState.count;
        double delta2 = minutiaeCount - currentState.mean;
        currentState.m2 += delta * delta2;

        currentState.lastTimestamp = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        if (currentState.count >= 10) {
            double variance = currentState.m2 / (currentState.count - 1);
            double stddev = Math.sqrt(variance);

            if (stddev > STDDEV_OVER_ALLOWANCE) {
                emitFeature(ctx.getCurrentKey(), stddev, minutiaeCount, currentState, out);
            }
        }

        state.update(currentState);
    }

    private void emitFeature(String key, double stddev, double currentCount, MinutiaeState state, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("bio_probe_minutiae_anomaly_v1");
        feature.setFeatureType("Variance");
        feature.setFeatureValue(stddev);
        feature.setWindowEnd(state.lastTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("standard_deviation", stddev);
        comments.put("current_minutiae", currentCount);
        comments.put("mean", state.mean);
        comments.put("event_count", state.count);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
