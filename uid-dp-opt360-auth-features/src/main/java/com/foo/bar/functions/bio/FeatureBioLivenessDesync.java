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
 * Feature: Bio Liveness Desync
 * Detects Liveness Token Replay attacks where the claimed liveness session
 * actually occurred > 60 minutes before the transaction hit Kafka.
 */
public class FeatureBioLivenessDesync extends KeyedProcessFunction<String, InputMessageBio, OutMessage> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long DESYNC_THRESHOLD_MS = 60 * 60 * 1000L; // 60 mins

    public static class DesyncState {
        public String lastProcessedEventId = "";
    }

    private transient ValueState<DesyncState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureBioLivenessDesyncDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureBioLivenessDesyncDescriptor);
    }

    @Override
    public void processElement(InputMessageBio bio, Context ctx, Collector<OutMessage> out) throws Exception {
        DesyncState currentState = state.value();
        if (currentState == null) currentState = new DesyncState();

        String eventId = bio.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) return;
            currentState.lastProcessedEventId = eventId;
        }

        state.update(currentState);

        String livenessEndStr = bio.getBiolivenessEndtime();
        if (livenessEndStr == null || livenessEndStr.isEmpty()) return;

        try {
            long livenessEndTs = Long.parseLong(livenessEndStr);
            long kafkaTs = bio.getKafkaSourceTimestamp();
            
            if (kafkaTs == 0) kafkaTs = ctx.timestamp();

            long delta = kafkaTs - livenessEndTs;
            if (delta > DESYNC_THRESHOLD_MS) {
                emitFeature(ctx.getCurrentKey(), delta, kafkaTs, out);
            }
        } catch (NumberFormatException e) {
            // Ignore parse failures
        }
    }

    private void emitFeature(String key, long deltaMs, long currentTs, Collector<OutMessage> out) {
        OutMessage f = new OutMessage();
        f.setOptId(key);
        f.setFeature("bio_liveness_desync_replay_v1");
        f.setFeatureType("Duration (mins)");
        f.setFeatureValue((double) deltaMs / (60 * 1000L));
        f.setWindowEnd(currentTs);
        f.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("desync_gap_mins", deltaMs / (60 * 1000L));
        comments.put("threshold_mins", DESYNC_THRESHOLD_MS / (60 * 1000L));

        try {
            f.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            f.setComments(comments.toString());
        }
        f.setSkipComments(true);
        out.collect(f);
    }
}
