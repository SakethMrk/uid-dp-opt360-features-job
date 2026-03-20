package com.foo.bar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Feature: Detects a Brute Force Compromise.
 * Pattern: Multiple 'N' (failures) followed by a 'Y' (success) within a tight time window.
 * Fault-Tolerance: Uses strict Event-Time buffering via MapState and OnTimers 
 * to handle any out-of-order event arrivals flawlessly.
 */
public class FeatureAuthBruteForceCompromise extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    // Pattern parameters
    private static final long PATTERN_WINDOW_MS = 5 * 60 * 1000L; // 5 minutes
    private static final int MIN_FAILURES_BEFORE_SUCCESS = 5;

    // 1. Buffer for handling Out-of-Order Events
    // Key: Event Timestamp, Value: List of events at that exact millisecond
    private transient MapState<Long, List<InputMessageTxn>> eventBuffer;

    // 2. State for the Business Logic (evaluated only in strict order)
    public static class BruteForceState {
        // Keeps the timeline of recent events within the PATTERN_WINDOW_MS
        public LinkedList<InputMessageTxn> recentEvents = new LinkedList<>();
    }
    private transient ValueState<BruteForceState> logicState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        StateDescriptors.featureAuthBruteForceCompromiseDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction1Day());
        eventBuffer = getRuntimeContext().getMapState(StateDescriptors.featureAuthBruteForceCompromiseBufferDescriptor);
        
        logicState = getRuntimeContext().getState(StateDescriptors.featureAuthBruteForceCompromiseDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String result = txn.getAuthResult();
        if (result == null || result.trim().isEmpty()) return;

        long eventTime = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        // 1. Buffer the event at its exact event time
        List<InputMessageTxn> eventsAtTime = eventBuffer.get(eventTime);
        if (eventsAtTime == null) {
            eventsAtTime = new ArrayList<>();
        }
        eventsAtTime.add(txn);
        eventBuffer.put(eventTime, eventsAtTime);

        // 2. Register a timer exactly at the event time.
        // Flink's WatermarkStrategy is already configured for bounded out-of-orderness (e.g., -60s).
        // Therefore, when the Watermark reaches `eventTime`, Flink GUARANTEES that no more events 
        // with timestamp <= eventTime will arrive (barring extremely late data).
        ctx.timerService().registerEventTimeTimer(eventTime);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OutMessage> out) throws Exception {
        // The Watermark has reached 'timestamp'. 
        // We can safely process all buffered events with timestamp <= 'timestamp' in strict order.

        // 1. Extract and Sort Buffered Events
        List<Map.Entry<Long, List<InputMessageTxn>>> toProcess = new ArrayList<>();
        for (Map.Entry<Long, List<InputMessageTxn>> entry : eventBuffer.entries()) {
            if (entry.getKey() <= timestamp) {
                toProcess.add(entry);
            }
        }
        
        // Sort ascending by key (timestamp)
        toProcess.sort(Comparator.comparingLong(Map.Entry::getKey));

        // 2. Feed strictly ordered events into the business logic
        for (Map.Entry<Long, List<InputMessageTxn>> entry : toProcess) {
            for (InputMessageTxn txn : entry.getValue()) {
                evaluateSequentialLogic(txn, entry.getKey(), ctx.getCurrentKey(), out);
            }
            // 3. Remove processed events from the buffer
            eventBuffer.remove(entry.getKey());
        }
    }

    /**
     * This method is guaranteed to receive events in PERFECT chronological order.
     */
    private void evaluateSequentialLogic(InputMessageTxn txn, long currentTs, String key, Collector<OutMessage> out) throws Exception {
        BruteForceState state = logicState.value();
        if (state == null) {
            state = new BruteForceState();
        }

        // Add newest event
        state.recentEvents.addLast(txn);

        // Prune events outside the 5-minute rolling window
        long cutoffTs = currentTs - PATTERN_WINDOW_MS;
        state.recentEvents.removeIf(e -> getEventTime(e) < cutoffTs);

        // Check the pattern: If current event is Success ('Y'), check history for trailing failures
        if ("Y".equalsIgnoreCase(txn.getAuthResult())) {
            int recentFailures = 0;
            
            // Traverse backward from the second-to-last element (the ones immediately preceding this success)
            ListIterator<InputMessageTxn> iter = state.recentEvents.listIterator(state.recentEvents.size() - 1);
            while (iter.hasPrevious()) {
                InputMessageTxn prevTxn = iter.previous();
                if ("N".equalsIgnoreCase(prevTxn.getAuthResult())) {
                    recentFailures++;
                } else {
                    // Broke the failure streak, stop counting backward
                    break;
                }
            }

            if (recentFailures >= MIN_FAILURES_BEFORE_SUCCESS) {
                // ANOMALY DETECTED: Brute force compromise
                emitFeature(key, recentFailures, currentTs, out);
                
                // Clear the state so we don't alert multiple times for the same sequence
                state.recentEvents.clear();
            }
        }

        logicState.update(state);
    }

    private long getEventTime(InputMessageTxn txn) {
        // Fallback parser since we don't have direct ctx access in the list element natively
        try {
            if (txn.getReqDateTime() != null) {
                return java.time.LocalDateTime.parse(txn.getReqDateTime(), com.foo.bar.pipeline.Driver.Configurations.formatter)
                        .atZone(com.foo.bar.pipeline.Driver.Configurations.istZone)
                        .toInstant().toEpochMilli();
            }
        } catch (Exception ignored) {}
        return System.currentTimeMillis();
    }

    private void emitFeature(String key, int precedingFailures, long endTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_brute_force_compromise_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) precedingFailures); // The severity
        feature.setWindowStart(endTs - PATTERN_WINDOW_MS);
        feature.setWindowEnd(endTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("pattern_matched", "Multiple failures followed immediately by a success");
        comments.put("preceding_failures", precedingFailures);
        comments.put("window_ms", PATTERN_WINDOW_MS);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
