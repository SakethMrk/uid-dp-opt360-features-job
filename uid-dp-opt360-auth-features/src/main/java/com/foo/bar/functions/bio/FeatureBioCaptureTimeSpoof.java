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
 * Feature: Bio Capture Time Spoof
 * Detects if the delta between Liveness Initiated and Received is physically impossible (< 50ms),
 * indicating a raw file injection rather than active camera rendering.
 */
public class FeatureBioCaptureTimeSpoof extends KeyedProcessFunction<String, InputMessageBio, OutMessage> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long SPOOF_THRESHOLD_MS = 50L;

    public static class CaptureState {
        public String lastProcessedEventId = "";
    }

    private transient ValueState<CaptureState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureBioCaptureTimeSpoofDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureBioCaptureTimeSpoofDescriptor);
    }

    @Override
    public void processElement(InputMessageBio bio, Context ctx, Collector<OutMessage> out) throws Exception {
        CaptureState currentState = state.value();
        if (currentState == null) currentState = new CaptureState();

        String eventId = bio.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) return;
            currentState.lastProcessedEventId = eventId;
        }

        state.update(currentState);

        if (!"true".equalsIgnoreCase(bio.getIsLivenessCheckInvoked())) return;

        String initTimeStr = bio.getLivenessRequestInitiatedTime();
        String recvTimeStr = bio.getLivenessResponseReceivedTime();

        if (initTimeStr == null || recvTimeStr == null || initTimeStr.isEmpty() || recvTimeStr.isEmpty()) return;

        try {
            long initTime = Long.parseLong(initTimeStr);
            long recvTime = Long.parseLong(recvTimeStr);

            long delta = recvTime - initTime;
            // Physical cameras require at least ~150-200ms to open, expose, and capture.
            if (delta > 0 && delta < SPOOF_THRESHOLD_MS) {
                emitFeature(ctx.getCurrentKey(), delta, ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis(), out);
            }
        } catch (NumberFormatException e) {
            // Unparsable times
        }
    }

    private void emitFeature(String key, long deltaMs, long currentTs, Collector<OutMessage> out) {
        OutMessage f = new OutMessage();
        f.setOptId(key);
        f.setFeature("bio_capture_time_spoof_v1");
        f.setFeatureType("Duration (ms)");
        f.setFeatureValue((double) deltaMs);
        f.setWindowEnd(currentTs);
        f.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("capture_delta_ms", deltaMs);
        comments.put("impossible_threshold_ms", SPOOF_THRESHOLD_MS);

        try {
            f.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            f.setComments(comments.toString());
        }
        f.setSkipComments(true);
        out.collect(f);
    }
}
