package com.foo.bar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class FeatureAuthDeviceDiversity extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long WINDOW_MS = 2 * 60 * 60 * 1000L; // 2 hours

    public static class DeviceState {
        public long windowStart = 0;
        public Set<String> devices = new HashSet<>();
        public boolean alerted = false;
    }

    private transient ValueState<DeviceState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthDeviceDiversityDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthDeviceDiversityDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String deviceCode = txn.getDeviceCode();
        if (deviceCode == null || deviceCode.trim().isEmpty()) return;

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        DeviceState currentState = state.value();
        if (currentState == null) {
            currentState = new DeviceState();
            currentState.windowStart = currentTs;
        } else if (currentTs - currentState.windowStart > WINDOW_MS) {
            currentState = new DeviceState(); // Start fresh tumbling window
            currentState.windowStart = currentTs;
        }

        currentState.devices.add(deviceCode);

        // Alert if an operator uses > 3 distinct devices in 2 hours
        if (currentState.devices.size() > 3 && !currentState.alerted) {
             emitFeature(ctx.getCurrentKey(), currentState.devices, currentTs, currentState.windowStart, out);
             currentState.alerted = true;
        }

        state.update(currentState);
    }

    private void emitFeature(String key, Set<String> codes, long currentTs, long windowStart, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_device_diversity_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) codes.size());
        feature.setWindowStart(windowStart);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("distinct_devices", codes.size());
        comments.put("device_codes", new ArrayList<>(codes));

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
