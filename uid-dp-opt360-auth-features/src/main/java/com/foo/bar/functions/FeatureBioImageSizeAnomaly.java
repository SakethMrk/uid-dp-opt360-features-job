package com.foo.bar.functions;

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

public class FeatureBioImageSizeAnomaly extends KeyedProcessFunction<String, InputMessageBio, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final double Z_SCORE_THRESHOLD = 3.0;

    public static class SizeState {
        public long count = 0;
        public double mean = 0.0;
        public double m2 = 0.0;
        public long lastTimestamp = 0;
    }

    private transient ValueState<SizeState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureBioImageSizeAnomalyDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureBioImageSizeAnomalyDescriptor);
    }

    @Override
    public void processElement(InputMessageBio bio, Context ctx, Collector<OutMessage> out) throws Exception {
        String sizeStr = bio.getImageSize();
        if (sizeStr == null || sizeStr.trim().isEmpty() || "null".equalsIgnoreCase(sizeStr)) return;

        double imageSize;
        try {
            imageSize = Double.parseDouble(sizeStr.trim());
        } catch (NumberFormatException e) {
            return;
        }

        if (imageSize <= 0) return;

        SizeState currentState = state.value();
        if (currentState == null) currentState = new SizeState();

        // Welford's online algorithm
        currentState.count++;
        double delta = imageSize - currentState.mean;
        currentState.mean += delta / currentState.count;
        double delta2 = imageSize - currentState.mean;
        currentState.m2 += delta * delta2;

        currentState.lastTimestamp = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        if (currentState.count >= 10) {
            double variance = currentState.m2 / (currentState.count - 1);
            double stddev = Math.sqrt(variance);

            if (stddev > 0) {
                double zScore = Math.abs(imageSize - currentState.mean) / stddev;
                if (zScore > Z_SCORE_THRESHOLD) {
                    emitFeature(ctx.getCurrentKey(), zScore, imageSize, currentState, out);
                }
            }
        }

        state.update(currentState);
    }

    private void emitFeature(String key, double zScore, double currentSize, SizeState state, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("bio_image_size_anomaly_v1");
        feature.setFeatureType("Z-Score");
        feature.setFeatureValue(zScore);
        feature.setWindowEnd(state.lastTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("z_score", zScore);
        comments.put("image_size", currentSize);
        comments.put("mean", state.mean);
        comments.put("stddev", Math.sqrt(state.m2 / (state.count > 1 ? state.count - 1 : 1)));
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
