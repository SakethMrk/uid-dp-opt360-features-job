package com.foo.bar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class FeatureAuthServerConcentration extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long WINDOW_MS = 60 * 60 * 1000L; // 1 hour

    public static class ServerState {
        public long windowStart = 0;
        public Map<String, Integer> serverCounts = new HashMap<>();
        public int totalCount = 0;
        public boolean alerted = false;
    }

    private transient ValueState<ServerState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthServerConcentrationDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthServerConcentrationDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String env = txn.getServerId();
        if (env == null || env.trim().isEmpty()) return;

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        ServerState currentState = state.value();
        if (currentState == null) {
            currentState = new ServerState();
            currentState.windowStart = currentTs;
        } else if (currentTs - currentState.windowStart > WINDOW_MS) {
            currentState = new ServerState();
            currentState.windowStart = currentTs;
        }

        currentState.serverCounts.put(env, currentState.serverCounts.getOrDefault(env, 0) + 1);
        currentState.totalCount++;

        if (currentState.totalCount >= 50) {
            int maxCount = 0;
            String dominantEnv = "";
            for (Map.Entry<String, Integer> entry : currentState.serverCounts.entrySet()) {
                if (entry.getValue() > maxCount) {
                    maxCount = entry.getValue();
                    dominantEnv = entry.getKey();
                }
            }

            double concentration = (double) maxCount / currentState.totalCount;

            if (concentration > 0.95 && !currentState.alerted) {
                 emitFeature(ctx.getCurrentKey(), dominantEnv, concentration, currentState, currentTs, out);
                 currentState.alerted = true;
            } else if (concentration <= 0.95) {
                 currentState.alerted = false;
            }
        }

        state.update(currentState);
    }

    private void emitFeature(String key, String env, double concentration, ServerState state, long currentTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_server_concentration_v1");
        feature.setFeatureType("Ratio");
        feature.setFeatureValue(concentration);
        feature.setWindowStart(state.windowStart);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("concentration_ratio", concentration);
        comments.put("dominant_server", env);
        comments.put("total_requests", state.totalCount);
        comments.put("server_counts", state.serverCounts);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
