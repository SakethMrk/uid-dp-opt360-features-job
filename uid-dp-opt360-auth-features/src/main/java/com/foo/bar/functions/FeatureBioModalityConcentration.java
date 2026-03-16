package com.foo.bar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageBio;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class FeatureBioModalityConcentration extends KeyedProcessFunction<String, InputMessageBio, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int WINDOW_SIZE = 30;
    private static final double ALERT_THRESHOLD = 0.95;

    public static class ModalityState {
        public Map<String, Integer> bioTypeCounts = new HashMap<>();
        public int total = 0;
        public long lastTimestamp = 0;
        public boolean alerted = false;
    }

    private transient ValueState<ModalityState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureBioModalityConcentrationDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureBioModalityConcentrationDescriptor);
    }

    @Override
    public void processElement(InputMessageBio bio, Context ctx, Collector<OutMessage> out) throws Exception {
        String type = bio.getBioType();
        if (type == null || type.trim().isEmpty() || "null".equalsIgnoreCase(type)) return;

        ModalityState currentState = state.value();
        if (currentState == null) currentState = new ModalityState();

        currentState.bioTypeCounts.put(type, currentState.bioTypeCounts.getOrDefault(type, 0) + 1);
        currentState.total++;
        currentState.lastTimestamp = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        if (currentState.total >= WINDOW_SIZE) {
            int maxCount = 0;
            String dominantType = "";
            for (Map.Entry<String, Integer> entry : currentState.bioTypeCounts.entrySet()) {
                if (entry.getValue() > maxCount) {
                    maxCount = entry.getValue();
                    dominantType = entry.getKey();
                }
            }

            double concentration = (double) maxCount / currentState.total;
            
            if (concentration > ALERT_THRESHOLD && !currentState.alerted) {
                emitFeature(ctx.getCurrentKey(), concentration, dominantType, currentState, out);
                currentState.alerted = true;
            } else if (concentration <= ALERT_THRESHOLD) {
                currentState.alerted = false; // reset
            }

            // Simple tumbling window reset behavior to prevent infinite growth
            if (currentState.total >= 100) {
                currentState = new ModalityState(); // clear for next chunk
            }
        }

        state.update(currentState);
    }

    private void emitFeature(String key, double concentration, String dominantType, ModalityState state, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("bio_modality_concentration_v1");
        feature.setFeatureType("Ratio");
        feature.setFeatureValue(concentration);
        feature.setWindowEnd(state.lastTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("concentration_ratio", concentration);
        comments.put("dominant_type", dominantType);
        comments.put("counts", state.bioTypeCounts);
        comments.put("window_size", state.total);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
