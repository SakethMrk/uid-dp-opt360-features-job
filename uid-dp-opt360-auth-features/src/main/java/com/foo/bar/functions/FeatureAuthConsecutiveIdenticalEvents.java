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
 * Feature: Packet Replay Detection - alerts if the exact same distinct payload 
 * is submitted consecutively 3+ times.
 * 
 * Source Fields Used: Event ID, AUA, SA, Auth Type, Error Code, Device Code, PID Size, FMR Count
 */
public class FeatureAuthConsecutiveIdenticalEvents extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int DUPLICATE_THRESHOLD = 3;

    public static class IdenticalState {
        public String lastSignature = "";
        public int consecutiveCount = 0;
        public boolean alerted = false;
        public String lastProcessedEventId = "";
        public long lastTs = 0;
    }

    private transient ValueState<IdenticalState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthConsecutiveIdenticalEventsDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction1Day());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthConsecutiveIdenticalEventsDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        IdenticalState currentState = state.value();
        if (currentState == null) currentState = new IdenticalState();

        String eventId = txn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) return;
            currentState.lastProcessedEventId = eventId;
        }

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();
        if (currentState.lastTs > 0 && currentTs <= currentState.lastTs) {
            currentTs = currentState.lastTs + 1;
        }

        String signature = safe(txn.getAua()) + "|" + 
                           safe(txn.getSa()) + "|" + 
                           safe(txn.getAuthType()) + "|" + 
                           safe(txn.getErrorCode()) + "|" + 
                           safe(txn.getDeviceCode()) + "|" + 
                           safe(txn.getPidSize()) + "|" + 
                           safe(txn.getFmrCount());

        if (signature.equals(currentState.lastSignature)) {
            currentState.consecutiveCount++;
        } else {
            currentState.lastSignature = signature;
            currentState.consecutiveCount = 1;
            currentState.alerted = false;
        }

        if (currentState.consecutiveCount >= DUPLICATE_THRESHOLD && !currentState.alerted) {
            emitFeature(ctx.getCurrentKey(), signature, currentState.consecutiveCount, currentTs, out);
            currentState.alerted = true;
        }

        currentState.lastTs = currentTs;
        state.update(currentState);
    }

    private String safe(String val) {
        return (val == null || val.trim().isEmpty()) ? "null" : val;
    }

    private void emitFeature(String key, String sig, int matches, long currentTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_packet_replay_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) matches);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("identical_consecutive_requests", matches);
        comments.put("packet_signature", sig);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
