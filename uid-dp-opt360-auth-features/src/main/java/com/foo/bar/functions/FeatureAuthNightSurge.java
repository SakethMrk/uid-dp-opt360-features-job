package com.foo.bar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import com.foo.bar.pipeline.Driver;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Feature: Night Surge - monitors for an extremely high percentage (>40%) of the 
 * operator's recent transaction volume occurring from 11 PM to 5 AM IST.
 * 
 * Source Fields Used: Event ID, Request DateTime
 */
public class FeatureAuthNightSurge extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int ROLLING_WINDOW_SIZE = 200;
    private static final double SURGE_THRESHOLD = 0.40;

    public static class SurgeState {
        public LinkedList<Boolean> isNightList = new LinkedList<>();
        public int nightCount = 0;
        public boolean alerted = false;
        public String lastProcessedEventId = "";
        public long lastTs = 0;
    }

    private transient ValueState<SurgeState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthNightSurgeDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction1Day());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthNightSurgeDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        SurgeState currentState = state.value();
        if (currentState == null) currentState = new SurgeState();

        String eventId = txn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) return;
            currentState.lastProcessedEventId = eventId;
        }

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();
        if (currentState.lastTs > 0 && currentTs <= currentState.lastTs) {
            currentTs = currentState.lastTs + 1;
        }

        ZonedDateTime zdt = Instant.ofEpochMilli(currentTs).atZone(Driver.Configurations.istZone);
        int hour = zdt.getHour();
        
        // 11 PM (23) through 4:59 AM (4)
        boolean isNight = (hour == 23 || hour >= 0 && hour <= 4);

        if (isNight) currentState.nightCount++;
        currentState.isNightList.addLast(isNight);

        if (currentState.isNightList.size() > ROLLING_WINDOW_SIZE) {
            boolean removed = currentState.isNightList.removeFirst();
            if (removed) currentState.nightCount--;
        }

        if (currentState.isNightList.size() >= 50) { // Min 50 requests before evaluating
            double ratio = (double) currentState.nightCount / currentState.isNightList.size();

            if (ratio >= SURGE_THRESHOLD && !currentState.alerted) {
                emitFeature(ctx.getCurrentKey(), ratio, currentState.nightCount, currentState.isNightList.size(), currentTs, out);
                currentState.alerted = true;
            } else if (ratio < SURGE_THRESHOLD) {
                currentState.alerted = false;
            }
        }

        currentState.lastTs = currentTs;
        state.update(currentState);
    }

    private void emitFeature(String key, double ratio, int nightCount, int totalCount, long currentTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_night_surge_v1");
        feature.setFeatureType("Ratio");
        feature.setFeatureValue(ratio);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("night_transactions", nightCount);
        comments.put("total_sampled", totalCount);
        comments.put("surge_ratio", ratio);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
