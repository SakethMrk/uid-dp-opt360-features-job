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

public class FeatureAuthLocationResidentMismatch extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int WINDOW_SIZE = 20;
    private static final double ALERT_THRESHOLD = 0.60;

    public static class MismatchState {
        public LinkedList<Boolean> recentMismatches = new LinkedList<>();
        public int mismatchCount = 0;
        public long lastTimestamp = 0;
        public boolean alerted = false;
    }

    private transient ValueState<MismatchState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthLocationResidentMismatchDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthLocationResidentMismatchDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String locState = txn.getLocationStateCode();
        String resState = txn.getResidentStateCode();
        
        if (locState == null || resState == null || locState.trim().isEmpty() || resState.trim().isEmpty()) {
            return;
        }

        MismatchState currentState = state.value();
        if (currentState == null) currentState = new MismatchState();

        boolean isMismatch = !locState.trim().equalsIgnoreCase(resState.trim());
        
        currentState.recentMismatches.addLast(isMismatch);
        if (isMismatch) currentState.mismatchCount++;

        if (currentState.recentMismatches.size() > WINDOW_SIZE) {
            boolean removed = currentState.recentMismatches.removeFirst();
            if (removed) currentState.mismatchCount--;
        }

        currentState.lastTimestamp = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        if (currentState.recentMismatches.size() >= 10) {
            double mismatchRatio = (double) currentState.mismatchCount / currentState.recentMismatches.size();
            
            if (mismatchRatio >= ALERT_THRESHOLD && !currentState.alerted) {
                emitFeature(ctx.getCurrentKey(), currentState, mismatchRatio, out);
                currentState.alerted = true;
            } else if (mismatchRatio < ALERT_THRESHOLD) {
                currentState.alerted = false;
            }
        }

        state.update(currentState);
    }

    private void emitFeature(String key, MismatchState state, double ratio, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_location_resident_mismatch_v1");
        feature.setFeatureType("Ratio");
        feature.setFeatureValue(ratio);
        feature.setWindowEnd(state.lastTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("mismatch_count", state.mismatchCount);
        comments.put("total_events", state.recentMismatches.size());
        comments.put("mismatch_ratio", ratio);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
