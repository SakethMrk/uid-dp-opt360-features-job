package com.foo.bar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

public class FeatureAuthFailureBurst extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long WINDOW_MS = 30 * 1000L; // 30 seconds
    private static final int BURST_THRESHOLD = 8;

    public static class BurstState {
        public LinkedList<Long> failureTimestamps = new LinkedList<>();
        public boolean alerted = false;
    }

    private transient ValueState<BurstState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthFailureBurstDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction1Day());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthFailureBurstDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        if (!"N".equalsIgnoreCase(txn.getAuthResult())) return;

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        BurstState currentState = state.value();
        if (currentState == null) currentState = new BurstState();

        currentState.failureTimestamps.addLast(currentTs);

        // Prune older than 30s
        long cutoff = currentTs - WINDOW_MS;
        Iterator<Long> iter = currentState.failureTimestamps.iterator();
        while (iter.hasNext()) {
            if (iter.next() < cutoff) {
                iter.remove();
            } else {
                break;
            }
        }

        int currentCount = currentState.failureTimestamps.size();
        
        if (currentCount >= BURST_THRESHOLD && !currentState.alerted) {
            emitFeature(ctx.getCurrentKey(), currentCount, currentTs, out);
            currentState.alerted = true;
        } else if (currentCount < BURST_THRESHOLD / 2) {
            currentState.alerted = false;
        }

        state.update(currentState);
        ctx.timerService().registerEventTimeTimer(currentTs + WINDOW_MS + 1000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OutMessage> out) throws Exception {
        BurstState currentState = state.value();
        if (currentState != null) {
            long cutoff = timestamp - WINDOW_MS;
            currentState.failureTimestamps.removeIf(ts -> ts < cutoff);
            if (currentState.failureTimestamps.isEmpty()) {
                state.clear();
            } else {
                state.update(currentState);
            }
        }
    }

    private void emitFeature(String key, int count, long endTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_failure_burst_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) count);
        feature.setWindowStart(endTs - WINDOW_MS);
        feature.setWindowEnd(endTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("failures_per_30s", count);
        comments.put("threshold", BURST_THRESHOLD);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
