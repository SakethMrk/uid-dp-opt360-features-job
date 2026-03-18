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
import java.util.LinkedList;
import java.util.Map;

/**
 * Feature: Biometric Replay - alerts if the exact same biometric signature (FM/FI counts, score, size) 
 * is continually re-submitted.
 * 
 * Source Fields Used: Event ID, fmrCount, firCount, fingerMatchScore, faceMatchScore, pidSize
 */
public class FeatureAuthBiometricReplay extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int MAX_BUFFER_SIZE = 50;

    public static class ReplayState {
        public LinkedList<String> recentSignatures = new LinkedList<>();
        public boolean alerted = false;
        public String lastProcessedEventId = "";
        public long lastTs = 0;
    }

    private transient ValueState<ReplayState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthBiometricReplayDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthBiometricReplayDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String fmr = txn.getFmrCount() != null ? txn.getFmrCount() : "0";
        String fir = txn.getFirCount() != null ? txn.getFirCount() : "0";
        
        // Only run on bio events
        if ("0".equals(fmr) && "0".equals(fir)) return;

        ReplayState currentState = state.value();
        if (currentState == null) currentState = new ReplayState();

        String eventId = txn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) return;
            currentState.lastProcessedEventId = eventId;
        }

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();
        if (currentState.lastTs > 0 && currentTs <= currentState.lastTs) {
            currentTs = currentState.lastTs + 1;
        }

        String signature = fmr + "|" + fir + "|" + 
                           (txn.getFingerMatchScore() != null ? txn.getFingerMatchScore() : "0") + "|" + 
                           (txn.getFaceMatchScore() != null ? txn.getFaceMatchScore() : "0") + "|" + 
                           (txn.getPidSize() != null ? txn.getPidSize() : "0");

        int matchCount = 0;
        for (String sig : currentState.recentSignatures) {
            if (sig.equals(signature)) {
                matchCount++;
            }
        }

        currentState.recentSignatures.addLast(signature);
        if (currentState.recentSignatures.size() > MAX_BUFFER_SIZE) {
            currentState.recentSignatures.removeFirst();
        }

        // If this exact signature has been seen 3 or more times recently
        if (matchCount >= 2 && !currentState.alerted) {
             emitFeature(ctx.getCurrentKey(), signature, matchCount + 1, currentTs, out);
             currentState.alerted = true; 
        } else if (matchCount < 2) {
             currentState.alerted = false;
        }

        currentState.lastTs = currentTs;
        state.update(currentState);
    }

    private void emitFeature(String key, String sig, int matches, long currentTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_biometric_replay_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) matches);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("exact_signature_matches", matches);
        comments.put("signature_blob", sig);
        comments.put("buffer_size", MAX_BUFFER_SIZE);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
