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

public class FeatureAuthModelDiversity extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long WINDOW_MS = 4 * 60 * 60 * 1000L; // 4 hours

    public static class ModelState {
        public long windowStart = 0;
        public Set<String> models = new HashSet<>();
        public boolean alerted = false;
    }

    private transient ValueState<ModelState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthModelDiversityDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthModelDiversityDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String modelId = txn.getModelId();
        if (modelId == null || modelId.trim().isEmpty()) return;

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        ModelState currentState = state.value();
        if (currentState == null) {
            currentState = new ModelState();
            currentState.windowStart = currentTs;
        } else if (currentTs - currentState.windowStart > WINDOW_MS) {
            currentState = new ModelState();
            currentState.windowStart = currentTs;
        }

        currentState.models.add(modelId);

        if (currentState.models.size() > 2 && !currentState.alerted) {
             emitFeature(ctx.getCurrentKey(), currentState.models, currentTs, currentState.windowStart, out);
             currentState.alerted = true;
        }

        state.update(currentState);
    }

    private void emitFeature(String key, Set<String> models, long currentTs, long windowStart, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_model_diversity_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) models.size());
        feature.setWindowStart(windowStart);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("distinct_models", models.size());
        comments.put("model_ids", new ArrayList<>(models));

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
