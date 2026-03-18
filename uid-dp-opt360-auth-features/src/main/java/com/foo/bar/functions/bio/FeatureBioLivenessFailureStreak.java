package com.foo.bar.functions.bio;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageBio;
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

public class FeatureBioLivenessFailureStreak extends KeyedProcessFunction<String, InputMessageBio, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static class StreakState {
        public int streakCount = 0;
        public long firstFailureTs = 0;
        public long lastFailureTs = 0;
        public String bioType = "";
        public Set<String> deviceCodes = new HashSet<>();
    }

    private transient ValueState<StreakState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureBioLivenessFailureStreakDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureBioLivenessFailureStreakDescriptor);
    }

    @Override
    public void processElement(InputMessageBio bio, Context ctx, Collector<OutMessage> out) throws Exception {
        String isLive = bio.getIsLive();
        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        StreakState currentState = state.value();
        if (currentState == null) currentState = new StreakState();

        if (isLive != null && !"Y".equalsIgnoreCase(isLive)) {
            // FAILURE - extend streak
            if (currentState.streakCount == 0) {
                currentState.firstFailureTs = currentTs;
                currentState.bioType = bio.getBioType() != null ? bio.getBioType() : "UNKNOWN";
            }
            currentState.streakCount++;
            currentState.lastFailureTs = currentTs;
            if (bio.getDeviceCode() != null) {
                currentState.deviceCodes.add(bio.getDeviceCode());
            }
            state.update(currentState);

        } else if ("Y".equalsIgnoreCase(isLive)) {
            // SUCCESS - streak broken, emit if streak > 0
            if (currentState.streakCount > 0) {
                emitFeature(ctx.getCurrentKey(), currentState, out);
            }
            state.clear();
        }
    }

    private void emitFeature(String key, StreakState state, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("bio_liveness_failure_streak_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) state.streakCount);
        feature.setWindowStart(state.firstFailureTs);
        feature.setWindowEnd(state.lastFailureTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("streak_count", state.streakCount);
        comments.put("bio_type", state.bioType);
        comments.put("first_failure", state.firstFailureTs);
        comments.put("last_failure", state.lastFailureTs);
        comments.put("device_codes", state.deviceCodes);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
