package com.foo.bar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

public class FeatureAuthModalitySwitch extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int WINDOW_SIZE = 10;
    private static final int SWITCH_THRESHOLD = 6;

    public static class ModalityState {
        public LinkedList<String> modalities = new LinkedList<>();
        public int switchCount = 0;
        public long lastTimestamp = 0;
        public boolean alerted = false;
    }

    private transient ValueState<ModalityState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthModalitySwitchDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthModalitySwitchDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String authType = txn.getAuthType();
        if (authType == null || authType.trim().isEmpty()) return;

        ModalityState currentState = state.value();
        if (currentState == null) currentState = new ModalityState();

        if (currentState.modalities.size() > 0) {
            String prev = currentState.modalities.getLast();
            if (!prev.equals(authType)) {
                currentState.switchCount++;
            }
        }

        currentState.modalities.addLast(authType);

        if (currentState.modalities.size() > WINDOW_SIZE) {
            String removed = currentState.modalities.removeFirst();
            String next = currentState.modalities.getFirst();
            if (!removed.equals(next)) {
                currentState.switchCount--;
            }
        }

        currentState.lastTimestamp = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        if (currentState.modalities.size() >= WINDOW_SIZE) {
            if (currentState.switchCount >= SWITCH_THRESHOLD && !currentState.alerted) {
                emitFeature(ctx.getCurrentKey(), currentState.switchCount, currentState, out);
                currentState.alerted = true;
            } else if (currentState.switchCount < SWITCH_THRESHOLD - 2) {
                currentState.alerted = false;
            }
        }

        state.update(currentState);
    }

    private void emitFeature(String key, int switchCount, ModalityState state, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_modality_switch_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) switchCount);
        feature.setWindowEnd(state.lastTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("switch_count", switchCount);
        comments.put("window_size", WINDOW_SIZE);
        comments.put("recent_modalities", state.modalities);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
