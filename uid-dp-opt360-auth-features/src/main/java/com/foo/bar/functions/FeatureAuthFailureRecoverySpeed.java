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
import java.util.Map;

/**
 * Feature: Detects bot-like instant recovery from a long string of failures.
 * 
 * Source Fields Used: Event ID, Auth Result, Request DateTime
 */
public class FeatureAuthFailureRecoverySpeed extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int MIN_FAILURES_BEFORE_RECOVERY = 5;
    private static final long BOT_RECOVERY_SPEED_THRESHOLD_MS = 1000L; // 1 second

    public static class RecoveryState {
        public int consecutiveFailures = 0;
        public long lastFailureTs = 0;
        public String lastProcessedEventId = "";
        public long lastTs = 0;
    }

    private transient ValueState<RecoveryState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthFailureRecoverySpeedDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthFailureRecoverySpeedDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String result = txn.getAuthResult();
        if (result == null || result.trim().isEmpty()) return;

        RecoveryState currentState = state.value();
        if (currentState == null) currentState = new RecoveryState();

        String eventId = txn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) return;
            currentState.lastProcessedEventId = eventId;
        }

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();
        if (currentState.lastTs > 0 && currentTs <= currentState.lastTs) {
            currentTs = currentState.lastTs + 1;
        }

        boolean isFailure = "N".equalsIgnoreCase(result);

        if (isFailure) {
            currentState.consecutiveFailures++;
            currentState.lastFailureTs = currentTs;
        } else if ("Y".equalsIgnoreCase(result)) {
            if (currentState.consecutiveFailures >= MIN_FAILURES_BEFORE_RECOVERY) {
                long recoveryGap = currentTs - currentState.lastFailureTs;
                
                // If the operator successfully authenticates < 1000ms after failing 5 times
                if (recoveryGap > 0 && recoveryGap <= BOT_RECOVERY_SPEED_THRESHOLD_MS) {
                    emitFeature(ctx.getCurrentKey(), recoveryGap, currentState.consecutiveFailures, currentTs, out);
                }
            }
            // Reset streak on success
            currentState.consecutiveFailures = 0;
            currentState.lastFailureTs = 0;
        }

        currentState.lastTs = currentTs;
        state.update(currentState);
    }

    private void emitFeature(String key, long recoveryGapMs, int prevFailures, long currentTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_bot_failure_recovery_speed_v1");
        feature.setFeatureType("Duration (ms)");
        feature.setFeatureValue((double) recoveryGapMs);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("recovery_gap_ms", recoveryGapMs);
        comments.put("prior_consecutive_failures", prevFailures);
        comments.put("threshold_ms", BOT_RECOVERY_SPEED_THRESHOLD_MS);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
