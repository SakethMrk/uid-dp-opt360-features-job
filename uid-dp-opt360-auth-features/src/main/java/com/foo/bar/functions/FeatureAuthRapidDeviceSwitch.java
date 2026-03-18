package com.foo.bar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * Feature: Detects an operator rapidly cycling across multiple distinct physical devices 
 * within a tiny time window (e.g., 4+ devices in 10 minutes).
 * 
 * Source Fields Used: Event ID, Device Code, Request DateTime
 */
public class FeatureAuthRapidDeviceSwitch extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long WINDOW_MS = 10 * 60 * 1000L; // 10 mins
    private static final int DEVICE_THRESHOLD = 4;

    public static class DeviceEvent {
        public long ts;
        public String deviceCode;
        public DeviceEvent(long ts, String dc) { this.ts = ts; this.deviceCode = dc; }
    }

    public static class RapidSwitchState {
        public LinkedList<DeviceEvent> events = new LinkedList<>();
        public boolean alerted = false;
        public String lastProcessedEventId = "";
        public long lastTs = 0;
    }

    private transient ValueState<RapidSwitchState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthRapidDeviceSwitchDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction1Day());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthRapidDeviceSwitchDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String deviceCode = txn.getDeviceCode();
        if (deviceCode == null || deviceCode.trim().isEmpty()) return;

        RapidSwitchState currentState = state.value();
        if (currentState == null) currentState = new RapidSwitchState();

        String eventId = txn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) return;
            currentState.lastProcessedEventId = eventId;
        }

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();
        if (currentState.lastTs > 0 && currentTs <= currentState.lastTs) {
            currentTs = currentState.lastTs + 1;
        }

        currentState.events.addLast(new DeviceEvent(currentTs, deviceCode));

        long cutoff = currentTs - WINDOW_MS;
        Iterator<DeviceEvent> iter = currentState.events.iterator();
        while (iter.hasNext()) {
            if (iter.next().ts < cutoff) iter.remove();
            else break;
        }

        Set<String> uniqueDevices = new HashSet<>();
        for (DeviceEvent e : currentState.events) {
            uniqueDevices.add(e.deviceCode);
        }

        int count = uniqueDevices.size();
        if (count >= DEVICE_THRESHOLD && !currentState.alerted) {
             emitFeature(ctx.getCurrentKey(), uniqueDevices, currentTs, out);
             currentState.alerted = true;
        } else if (count < DEVICE_THRESHOLD - 1) {
             currentState.alerted = false;
        }

        currentState.lastTs = currentTs;
        state.update(currentState);
        
        ctx.timerService().registerEventTimeTimer(currentTs + WINDOW_MS + 1000);
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OutMessage> out) throws Exception {
        RapidSwitchState currentState = state.value();
        if (currentState != null) {
            long cutoff = timestamp - WINDOW_MS;
            currentState.events.removeIf(e -> e.ts < cutoff);
            if (currentState.events.isEmpty()) {
                state.clear();
            } else {
                state.update(currentState);
            }
        }
    }

    private void emitFeature(String key, Set<String> devices, long currentTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_rapid_device_switch_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) devices.size());
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("distinct_devices_10m", devices.size());
        comments.put("devices_seen", devices);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
