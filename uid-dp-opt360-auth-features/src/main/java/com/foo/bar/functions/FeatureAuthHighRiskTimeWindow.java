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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Feature: High Risk Time Window - alerts if an operator triggers excessive failures 
 * specifically in the dead of night (1 AM to 4 AM IST).
 * 
 * Source Fields Used: Event ID, Auth Result, Request DateTime
 */
public class FeatureAuthHighRiskTimeWindow extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long WINDOW_MS = 24 * 60 * 60 * 1000L; // 24 hours
    private static final int NIGHT_FAILURES_THRESHOLD = 5;

    public static class HighRiskState {
        public LinkedList<Long> nightFailures = new LinkedList<>();
        public boolean alerted = false;
        public String lastProcessedEventId = "";
        public long lastTs = 0;
    }

    private transient ValueState<HighRiskState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthHighRiskTimeWindowDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction1Day());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthHighRiskTimeWindowDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        if (!"N".equalsIgnoreCase(txn.getAuthResult())) return;

        HighRiskState currentState = state.value();
        if (currentState == null) currentState = new HighRiskState();

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
        
        // Filter for 1 AM to 4 AM
        if (hour >= 1 && hour < 4) {
            currentState.nightFailures.addLast(currentTs);
        }

        long cutoff = currentTs - WINDOW_MS;
        Iterator<Long> iter = currentState.nightFailures.iterator();
        while (iter.hasNext()) {
            if (iter.next() < cutoff) iter.remove();
            else break;
        }

        int count = currentState.nightFailures.size();
        if (count >= NIGHT_FAILURES_THRESHOLD && !currentState.alerted) {
            emitFeature(ctx.getCurrentKey(), count, currentTs, out);
            currentState.alerted = true;
        } else if (count < NIGHT_FAILURES_THRESHOLD / 2) {
            currentState.alerted = false;
        }

        currentState.lastTs = currentTs;
        state.update(currentState);
    }

    private void emitFeature(String key, int count, long currentTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_high_risk_time_window_fails_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) count);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("failures_between_1AM_and_4AM", count);
        comments.put("threshold", NIGHT_FAILURES_THRESHOLD);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
