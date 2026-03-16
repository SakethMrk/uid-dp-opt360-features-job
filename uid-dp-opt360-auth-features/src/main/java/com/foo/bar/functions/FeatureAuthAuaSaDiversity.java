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

public class FeatureAuthAuaSaDiversity extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long WINDOW_MS = 2 * 60 * 60 * 1000L; // 2 hours

    public static class AuaSaState {
        public long windowStart = 0;
        public Set<String> pairs = new HashSet<>();
        public boolean alerted = false;
    }

    private transient ValueState<AuaSaState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthAuaSaDiversityDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthAuaSaDiversityDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String aua = txn.getAua();
        String sa = txn.getSa();
        
        if (aua == null || sa == null || aua.trim().isEmpty() || sa.trim().isEmpty()) return;

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        AuaSaState currentState = state.value();
        if (currentState == null) {
            currentState = new AuaSaState();
            currentState.windowStart = currentTs;
        } else if (currentTs - currentState.windowStart > WINDOW_MS) {
            currentState = new AuaSaState();
            currentState.windowStart = currentTs;
        }

        String pair = aua.trim() + ":" + sa.trim();
        currentState.pairs.add(pair);

        if (currentState.pairs.size() > 8 && !currentState.alerted) {
             emitFeature(ctx.getCurrentKey(), currentState.pairs, currentTs, currentState.windowStart, out);
             currentState.alerted = true;
        }

        state.update(currentState);
    }

    private void emitFeature(String key, Set<String> pairs, long currentTs, long windowStart, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_aua_sa_diversity_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) pairs.size());
        feature.setWindowStart(windowStart);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("distinct_pairs", pairs.size());
        comments.put("aua_sa_pairs", new ArrayList<>(pairs));

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
