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

public class FeatureAuthDeviceProviderSwitch extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long WINDOW_MS = 4 * 60 * 60 * 1000L; // 4 hours

    public static class ProviderState {
        public long windowStart = 0;
        public Set<String> providers = new HashSet<>();
        public boolean alerted = false;
    }

    private transient ValueState<ProviderState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthDeviceProviderSwitchDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthDeviceProviderSwitchDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String providerId = txn.getDeviceProviderId();
        if (providerId == null || providerId.trim().isEmpty()) return;

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        ProviderState currentState = state.value();
        if (currentState == null) {
            currentState = new ProviderState();
            currentState.windowStart = currentTs;
        } else if (currentTs - currentState.windowStart > WINDOW_MS) {
            currentState = new ProviderState();
            currentState.windowStart = currentTs;
        }

        currentState.providers.add(providerId);

        if (currentState.providers.size() > 1 && !currentState.alerted) {
             emitFeature(ctx.getCurrentKey(), currentState.providers, currentTs, currentState.windowStart, out);
             currentState.alerted = true;
        }

        state.update(currentState);
    }

    private void emitFeature(String key, Set<String> providers, long currentTs, long windowStart, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_device_provider_switch_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) providers.size());
        feature.setWindowStart(windowStart);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("distinct_providers", providers.size());
        comments.put("provider_ids", new ArrayList<>(providers));

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
