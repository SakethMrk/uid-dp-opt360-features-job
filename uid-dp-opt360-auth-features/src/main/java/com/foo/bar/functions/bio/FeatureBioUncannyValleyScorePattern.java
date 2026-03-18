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

/**
 * Feature: Bio Uncanny Valley Score Pattern
 * Detects deepfake bots generating completely uniform liveness scores with zero natural variance,
 * typically by targeting an API threshold exactly (e.g., repeating 0.985 exactly 5 times).
 */
public class FeatureBioUncannyValleyScorePattern extends KeyedProcessFunction<String, InputMessageBio, OutMessage> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int DUPLICATE_THRESHOLD = 5;

    public static class UncannyState {
        public String lastScore = "";
        public int consecutiveCount = 0;
        public boolean alerted = false;
        public String lastProcessedEventId = "";
        public long lastTs = 0;
    }

    private transient ValueState<UncannyState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureBioUncannyValleyScorePatternDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction1Day());
        state = getRuntimeContext().getState(StateDescriptors.featureBioUncannyValleyScorePatternDescriptor);
    }

    @Override
    public void processElement(InputMessageBio bio, Context ctx, Collector<OutMessage> out) throws Exception {
        String scoreStr = bio.getFaceLivenessScore();
        if (scoreStr == null || scoreStr.trim().isEmpty() || "null".equalsIgnoreCase(scoreStr)) return;

        UncannyState currentState = state.value();
        if (currentState == null) currentState = new UncannyState();

        String eventId = bio.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) return;
            currentState.lastProcessedEventId = eventId;
        }

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();
        if (currentState.lastTs > 0 && currentTs <= currentState.lastTs) {
            currentTs = currentState.lastTs + 1;
        }

        if (scoreStr.equals(currentState.lastScore)) {
            currentState.consecutiveCount++;
        } else {
            currentState.lastScore = scoreStr;
            currentState.consecutiveCount = 1;
            currentState.alerted = false;
        }

        if (currentState.consecutiveCount >= DUPLICATE_THRESHOLD && !currentState.alerted) {
            emitFeature(ctx.getCurrentKey(), scoreStr, currentState.consecutiveCount, currentTs, out);
            currentState.alerted = true;
        }

        currentState.lastTs = currentTs;
        state.update(currentState);
    }

    private void emitFeature(String key, String score, int matches, long currentTs, Collector<OutMessage> out) {
        OutMessage f = new OutMessage();
        f.setOptId(key);
        f.setFeature("bio_uncanny_valley_score_v1");
        f.setFeatureType("Count");
        f.setFeatureValue((double) matches);
        f.setWindowEnd(currentTs);
        f.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("consecutive_identical_liveness_scores", matches);
        comments.put("exact_score_value", score);

        try {
            f.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            f.setComments(comments.toString());
        }
        f.setSkipComments(true);
        out.collect(f);
    }
}
