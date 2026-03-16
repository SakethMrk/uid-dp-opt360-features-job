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
import java.util.LinkedHashMap;
import java.util.Map;

public class FeatureAuthTimeEntropy extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static class EntropyState {
        public int[] hourBuckets = new int[24];
        public int totalEvents = 0;
        public long lastTimestamp = 0;
    }

    private transient ValueState<EntropyState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthTimeEntropyDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction1Day());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthTimeEntropyDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();
        ZonedDateTime zdt = Instant.ofEpochMilli(currentTs).atZone(Driver.Configurations.istZone);
        int hour = zdt.getHour();

        EntropyState currentState = state.value();
        if (currentState == null) currentState = new EntropyState();

        currentState.hourBuckets[hour]++;
        currentState.totalEvents++;
        currentState.lastTimestamp = currentTs;

        // Evaluate every 50 events
        if (currentState.totalEvents > 0 && currentState.totalEvents % 50 == 0) {
            double entropy = computeShannonEntropy(currentState.hourBuckets, currentState.totalEvents);
            
            // Bot signals are extremely low (< 1.5) or abnormally distributed (> 4.2)
            if (entropy < 1.5 || entropy > 4.2) {
                emitFeature(ctx.getCurrentKey(), entropy, currentState, out);
            }
        }

        state.update(currentState);
    }

    private double computeShannonEntropy(int[] buckets, int total) {
        if (total == 0) return 0.0;
        double entropy = 0.0;
        for (int count : buckets) {
            if (count > 0) {
                double p = (double) count / total;
                entropy -= p * (Math.log(p) / Math.log(2)); // Log base 2
            }
        }
        return entropy;
    }

    private void emitFeature(String key, double entropy, EntropyState state, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_time_entropy_bits_v1");
        feature.setFeatureType("Entropy Bits");
        feature.setFeatureValue(entropy);
        feature.setWindowEnd(state.lastTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("entropy_bits", entropy);
        comments.put("total_events", state.totalEvents);
        comments.put("hour_distribution", state.hourBuckets); // Will serialize nicely as JSON array

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
