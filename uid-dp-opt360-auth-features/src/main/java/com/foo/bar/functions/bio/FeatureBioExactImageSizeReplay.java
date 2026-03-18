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
 * Feature: Bio Exact Image Size Replay
 * Detects 4+ consecutive biometric transactions containing the exact same byte-size image,
 * which is physically nearly impossible due to lighting noise, indicating static file upload.
 */
public class FeatureBioExactImageSizeReplay extends KeyedProcessFunction<String, InputMessageBio, OutMessage> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int DUPLICATE_THRESHOLD = 4;

    public static class ImageSizeState {
        public String lastImageSize = "";
        public int consecutiveCount = 0;
        public boolean alerted = false;
        public String lastProcessedEventId = "";
        public long lastTs = 0;
    }

    private transient ValueState<ImageSizeState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureBioExactImageSizeReplayDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureBioExactImageSizeReplayDescriptor);
    }

    @Override
    public void processElement(InputMessageBio bio, Context ctx, Collector<OutMessage> out) throws Exception {
        String sizeStr = bio.getImageSize();
        if (sizeStr == null || sizeStr.trim().isEmpty() || "0".equals(sizeStr) || "null".equalsIgnoreCase(sizeStr)) return;

        ImageSizeState currentState = state.value();
        if (currentState == null) currentState = new ImageSizeState();

        String eventId = bio.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) return;
            currentState.lastProcessedEventId = eventId;
        }

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();
        if (currentState.lastTs > 0 && currentTs <= currentState.lastTs) {
            currentTs = currentState.lastTs + 1;
        }

        if (sizeStr.equals(currentState.lastImageSize)) {
            currentState.consecutiveCount++;
        } else {
            currentState.lastImageSize = sizeStr;
            currentState.consecutiveCount = 1;
            currentState.alerted = false;
        }

        if (currentState.consecutiveCount >= DUPLICATE_THRESHOLD && !currentState.alerted) {
            emitFeature(ctx.getCurrentKey(), sizeStr, currentState.consecutiveCount, currentTs, out);
            currentState.alerted = true;
        }

        currentState.lastTs = currentTs;
        state.update(currentState);
    }

    private void emitFeature(String key, String imageSize, int matches, long currentTs, Collector<OutMessage> out) {
        OutMessage f = new OutMessage();
        f.setOptId(key);
        f.setFeature("bio_exact_image_size_replay_v1");
        f.setFeatureType("Count");
        f.setFeatureValue((double) matches);
        f.setWindowEnd(currentTs);
        f.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("consecutive_identical_sizes", matches);
        comments.put("exact_byte_size", imageSize);

        try {
            f.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            f.setComments(comments.toString());
        }
        f.setSkipComments(true);
        out.collect(f);
    }
}
