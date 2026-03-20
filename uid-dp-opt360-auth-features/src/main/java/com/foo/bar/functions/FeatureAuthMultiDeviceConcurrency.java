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
 * Feature: Detects Concurrent Multi-Device Logins for the same OptId.
 * Pattern: Logins from >= 3 distinct devices within a heavily restricted time window (10 seconds).
 * Fault-Tolerance: Strict Event-Time buffering guarantees accurate device distinct-counting 
 * even if events arrive out of order across the network.
 */
public class FeatureAuthMultiDeviceConcurrency extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    // Very tight window for concurrency
    private static final long CONCURRENCY_WINDOW_MS = 10 * 1000L; // 10 seconds
    private static final int MIN_DISTINCT_DEVICES = 3;

    // Buffer for Out-of-Order Handling
    private transient MapState<Long, List<InputMessageTxn>> eventBuffer;

    // Ordered Logic State
    public static class ConcurrencyState {
        // Storing timestamps and their associated device blocks
        public LinkedList<EventRecord> recentRecords = new LinkedList<>();
    }
    
    public static class EventRecord {
        public long timestamp;
        public String deviceCode;
        public EventRecord(long ts, String dc) {
            this.timestamp = ts;
            this.deviceCode = dc;
        }
    }
    
    private transient ValueState<ConcurrencyState> logicState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthMultiDeviceConcurrencyDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction1Day());
        eventBuffer = getRuntimeContext().getMapState(StateDescriptors.featureAuthMultiDevBufferDescriptor);
        logicState = getRuntimeContext().getState(StateDescriptors.featureAuthMultiDeviceConcurrencyDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String deviceCode = txn.getDeviceCode();
        if (deviceCode == null || deviceCode.trim().isEmpty()) return;

        long eventTime = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        List<InputMessageTxn> eventsAtTime = eventBuffer.get(eventTime);
        if (eventsAtTime == null) {
            eventsAtTime = new ArrayList<>();
        }
        eventsAtTime.add(txn);
        eventBuffer.put(eventTime, eventsAtTime);

        // Schedule timer to evaluate when we are certain no more events for 'eventTime' will arrive
        ctx.timerService().registerEventTimeTimer(eventTime);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OutMessage> out) throws Exception {
        List<Map.Entry<Long, List<InputMessageTxn>>> toProcess = new ArrayList<>();
        for (Map.Entry<Long, List<InputMessageTxn>> entry : eventBuffer.entries()) {
            if (entry.getKey() <= timestamp) {
                toProcess.add(entry);
            }
        }
        
        toProcess.sort(Comparator.comparingLong(Map.Entry::getKey));

        for (Map.Entry<Long, List<InputMessageTxn>> entry : toProcess) {
            for (InputMessageTxn txn : entry.getValue()) {
                evaluateSequentialLogic(txn, entry.getKey(), ctx.getCurrentKey(), out);
            }
            eventBuffer.remove(entry.getKey());
        }
    }

    private void evaluateSequentialLogic(InputMessageTxn txn, long currentTs, String key, Collector<OutMessage> out) throws Exception {
        ConcurrencyState state = logicState.value();
        if (state == null) {
            state = new ConcurrencyState();
        }

        // Add current event
        state.recentRecords.addLast(new EventRecord(currentTs, txn.getDeviceCode()));

        // Remove elements outside 10 second window
        long cutoffTs = currentTs - CONCURRENCY_WINDOW_MS;
        state.recentRecords.removeIf(record -> record.timestamp < cutoffTs);

        // Calculate distinct devices inside the current rolling window
        Set<String> distinctDevices = new HashSet<>();
        for (EventRecord record : state.recentRecords) {
            distinctDevices.add(record.deviceCode);
        }

        if (distinctDevices.size() >= MIN_DISTINCT_DEVICES) {
            emitFeature(key, distinctDevices, currentTs, out);
            
            // Clear to avoid alert storm for the same incident
            state.recentRecords.clear();
        }

        logicState.update(state);
    }

    private void emitFeature(String key, Set<String> distinctDevices, long endTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_multi_device_concurrency_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) distinctDevices.size());
        feature.setWindowStart(endTs - CONCURRENCY_WINDOW_MS);
        feature.setWindowEnd(endTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("alert_reason", "Concurrent auth attempts from highly disparate devices in a microscopic time window");
        comments.put("distinct_device_count", distinctDevices.size());
        comments.put("device_codes_involved", new ArrayList<>(distinctDevices));

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
