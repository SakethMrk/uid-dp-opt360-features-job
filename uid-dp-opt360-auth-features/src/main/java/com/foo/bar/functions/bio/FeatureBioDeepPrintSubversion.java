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
 * Feature: Bio DeepPrint Subversion 
 * Detects tampered APK clients forcing a `deepPrintIsMatched == true` boolean
 * while failing the underlying math (e.g. FIR Match Score is unnaturally low).
 */
public class FeatureBioDeepPrintSubversion extends KeyedProcessFunction<String, InputMessageBio, OutMessage> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final double FIR_IMPOSSIBLE_SUCCESS_THRESHOLD = 5.0; // Math fails, boolean succeeds

    public static class DeepPrintState {
        public String lastProcessedEventId = "";
        public long lastTs = 0;
    }

    private transient ValueState<DeepPrintState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureBioDeepPrintSubversionDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureBioDeepPrintSubversionDescriptor);
    }

    @Override
    public void processElement(InputMessageBio bio, Context ctx, Collector<OutMessage> out) throws Exception {
        DeepPrintState currentState = state.value();
        if (currentState == null) currentState = new DeepPrintState();

        String eventId = bio.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) return;
            currentState.lastProcessedEventId = eventId;
        }

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();
        if (currentState.lastTs > 0 && currentTs <= currentState.lastTs) {
            currentTs = currentState.lastTs + 1;
        }

        String isMatchedStr = bio.getDeepPrintIsMatched();
        if ("true".equalsIgnoreCase(isMatchedStr) || "Y".equalsIgnoreCase(isMatchedStr)) {
            String firScoreStr = bio.getDeepPrintFIRMatchScore();
            if (firScoreStr != null && !firScoreStr.isEmpty() && !"null".equalsIgnoreCase(firScoreStr)) {
                try {
                    double firScore = Double.parseDouble(firScoreStr);
                    if (firScore < FIR_IMPOSSIBLE_SUCCESS_THRESHOLD) {
                        emitFeature(ctx.getCurrentKey(), isMatchedStr, firScore, currentTs, out);
                    }
                } catch (NumberFormatException e) {
                    // Ignored
                }
            } else {
                // IsMatched is TRUE, but FIR Score is entirely missing (APK override)
                emitFeature(ctx.getCurrentKey(), isMatchedStr, 0.0, currentTs, out);
            }
        }

        currentState.lastTs = currentTs;
        state.update(currentState);
    }

    private void emitFeature(String key, String isMatched, double firScore, long currentTs, Collector<OutMessage> out) {
        OutMessage f = new OutMessage();
        f.setOptId(key);
        f.setFeature("bio_deep_print_subversion_v1");
        f.setFeatureType("Mismatch");
        f.setFeatureValue(1.0);
        f.setWindowEnd(currentTs);
        f.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("is_matched", isMatched);
        comments.put("actual_fir_match_score", firScore);
        comments.put("subversion_type", "Client boolean override detected");

        try {
            f.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            f.setComments(comments.toString());
        }
        f.setSkipComments(true);
        out.collect(f);
    }
}
