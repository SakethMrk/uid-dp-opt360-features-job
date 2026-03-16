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

public class FeatureAuthFmrCountAnomaly extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final double ALERT_STDDEV = 15.0;

    public static class FmrState {
        public LinkedList<Integer> counts = new LinkedList<>();
        public long lastTimestamp = 0;
        public boolean alerted = false;
    }

    private transient ValueState<FmrState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthFmrCountAnomalyDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthFmrCountAnomalyDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String fmrStr = txn.getFmrCount();
        if (fmrStr == null || fmrStr.trim().isEmpty()) return;

        int fmr;
        try {
            fmr = Integer.parseInt(fmrStr.trim());
        } catch (NumberFormatException e) {
            return;
        }

        if (fmr <= 0) return;

        FmrState currentState = state.value();
        if (currentState == null) currentState = new FmrState();

        currentState.counts.addLast(fmr);
        if (currentState.counts.size() > 10) {
            currentState.counts.removeFirst();
        }

        currentState.lastTimestamp = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        if (currentState.counts.size() >= 10) {
            double stddev = computeStdDev(currentState.counts);
            
            if (stddev >= ALERT_STDDEV && !currentState.alerted) {
                emitFeature(ctx.getCurrentKey(), stddev, currentState, out);
                currentState.alerted = true;
            } else if (stddev < ALERT_STDDEV / 2) {
                currentState.alerted = false;
            }
        }

        state.update(currentState);
    }

    private double computeStdDev(LinkedList<Integer> data) {
        double sum = 0;
        for (int val : data) sum += val;
        double mean = sum / data.size();

        double sqDiffSum = 0;
        for (int val : data) {
            sqDiffSum += Math.pow(val - mean, 2);
        }
        
        return Math.sqrt(sqDiffSum / data.size());
    }

    private void emitFeature(String key, double stddev, FmrState state, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_fmr_count_anomaly_v1");
        feature.setFeatureType("Variance");
        feature.setFeatureValue(stddev);
        feature.setWindowEnd(state.lastTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("stddev", stddev);
        comments.put("recent_counts", state.counts);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
