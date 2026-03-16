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

public class FeatureAuthEnrolmentHammering extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long WINDOW_MS = 60 * 60 * 1000L; // 1 hour

    public static class HammeringState {
        public LinkedList<Long> enrolments = new LinkedList<>();
        public boolean alerted = false;
    }

    private transient ValueState<HammeringState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthEnrolmentHammeringDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthEnrolmentHammeringDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String enrolmentRef = txn.getEnrolmentReferenceId();
        if (enrolmentRef == null || enrolmentRef.trim().isEmpty() || "null".equalsIgnoreCase(enrolmentRef)) return;

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        HammeringState currentState = state.value();
        if (currentState == null) currentState = new HammeringState();

        currentState.enrolments.addLast(currentTs);

        long cutoff = currentTs - WINDOW_MS;
        currentState.enrolments.removeIf(ts -> ts < cutoff);

        int count = currentState.enrolments.size();
        
        // 5+ enrolments in 1 hour is extremely suspicious
        if (count >= 5 && !currentState.alerted) {
             emitFeature(ctx.getCurrentKey(), count, currentTs, currentTs - WINDOW_MS, out);
             currentState.alerted = true;
        } else if (count < 3) {
             currentState.alerted = false;
        }

        state.update(currentState);
    }

    private void emitFeature(String key, int count, long currentTs, long windowStart, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_enrolment_hammering_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) count);
        feature.setWindowStart(windowStart);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("enrolment_count", count);
        comments.put("window", "1_hour");

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
