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
 * Feature: Detects non-human endurance (e.g., an operator actively authenticating 
 * continuously for 18+ hours without a 30-minute gap).
 * 
 * Source Fields Used: Event ID, Request DateTime 
 */
public class FeatureAuthGhostUptime extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long MAX_GAP_MS = 30 * 60 * 1000L; // 30 minutes
    private static final long GHOST_UPTIME_THRESHOLD_MS = 18 * 60 * 60 * 1000L; // 18 hours

    public static class UptimeState {
        public long sessionStartTs = 0;
        public long lastTs = 0;
        public boolean alerted = false;
        public String lastProcessedEventId = "";
    }

    private transient ValueState<UptimeState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthGhostUptimeDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthGhostUptimeDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        UptimeState currentState = state.value();
        if (currentState == null) currentState = new UptimeState();

        String eventId = txn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) return; // Dedup
            currentState.lastProcessedEventId = eventId;
        }

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();
        
        // Logical ordering guard
        if (currentState.lastTs > 0 && currentTs <= currentState.lastTs) {
            currentTs = currentState.lastTs + 1;
        }

        if (currentState.sessionStartTs == 0) {
            currentState.sessionStartTs = currentTs;
        } else {
            long gap = currentTs - currentState.lastTs;
            if (gap > MAX_GAP_MS) {
                // Operator took a break, reset session
                currentState.sessionStartTs = currentTs;
                currentState.alerted = false;
            } else {
                long totalUptime = currentTs - currentState.sessionStartTs;
                if (totalUptime >= GHOST_UPTIME_THRESHOLD_MS && !currentState.alerted) {
                    emitFeature(ctx.getCurrentKey(), totalUptime, currentTs, currentState, out);
                    currentState.alerted = true;
                }
            }
        }

        currentState.lastTs = currentTs;
        state.update(currentState);
        
        ctx.timerService().registerEventTimeTimer(currentTs + MAX_GAP_MS + 1000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OutMessage> out) throws Exception {
        UptimeState currentState = state.value();
        if (currentState != null) {
            long gap = timestamp - currentState.lastTs;
            if (gap > MAX_GAP_MS) {
                // Session expired, clear state to free memory
                state.clear();
            }
        }
    }

    private void emitFeature(String key, long uptimeMs, long currentTs, UptimeState state, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_ghost_uptime_v1");
        feature.setFeatureType("Duration (Hours)");
        feature.setFeatureValue((double) uptimeMs / (60 * 60 * 1000L));
        feature.setWindowStart(state.sessionStartTs);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("continuous_uptime_hours", (double) uptimeMs / (60 * 60 * 1000L));
        comments.put("max_allowed_gap_mins", 30);
        comments.put("threshold_hours", 18);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
