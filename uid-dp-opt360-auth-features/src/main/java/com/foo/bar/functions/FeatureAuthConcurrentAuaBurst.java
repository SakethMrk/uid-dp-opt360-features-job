package com.foo.bar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Feature: Detects an operator servicing multiple distinct AUA (Agencies) in the exact same millisecond.
 * 
 * Source Fields Used: Event ID, AUA, Event Timestamp
 */
public class FeatureAuthConcurrentAuaBurst extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long PRUNE_WINDOW_MS = 60 * 1000L; 

    public static class ConcurrentAuaState {
        // Maps precise timestamp -> set of AUAs
        public Map<Long, Set<String>> timestampToAuas = new LinkedHashMap<>();
        public String lastProcessedEventId = "";
        public boolean alertedForCurrentTs = false;
        public long lastAlertTs = 0;
    }

    private transient ValueState<ConcurrentAuaState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthConcurrentAuaBurstDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthConcurrentAuaBurstDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String aua = txn.getAua();
        if (aua == null || aua.trim().isEmpty()) return;

        ConcurrentAuaState currentState = state.value();
        if (currentState == null) currentState = new ConcurrentAuaState();

        String eventId = txn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) return;
            currentState.lastProcessedEventId = eventId;
        }

        long exactTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        Set<String> auas = currentState.timestampToAuas.computeIfAbsent(exactTs, k -> new HashSet<>());
        auas.add(aua);

        if (auas.size() >= 3) {
            if (currentState.lastAlertTs != exactTs || !currentState.alertedForCurrentTs) {
                emitFeature(ctx.getCurrentKey(), auas, exactTs, out);
                currentState.alertedForCurrentTs = true;
                currentState.lastAlertTs = exactTs;
            }
        }

        // Prune old timestamps to avoid map growth
        long cutoff = exactTs - PRUNE_WINDOW_MS;
        currentState.timestampToAuas.entrySet().removeIf(entry -> entry.getKey() < cutoff);

        state.update(currentState);
    }

    private void emitFeature(String key, Set<String> auas, long exactTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_concurrent_aua_burst_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) auas.size());
        feature.setWindowStart(exactTs);
        feature.setWindowEnd(exactTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("distinct_auas_in_same_ms", auas.size());
        comments.put("auas_list", auas);
        comments.put("exact_timestamp_ms", exactTs);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
