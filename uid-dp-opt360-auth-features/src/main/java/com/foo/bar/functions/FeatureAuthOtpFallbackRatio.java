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

public class FeatureAuthOtpFallbackRatio extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final int WINDOW_SIZE = 30;
    private static final double ALERT_THRESHOLD = 0.50;

    public static class OtpState {
        public LinkedList<String> recentAuthTypes = new LinkedList<>();
        public int otpCount = 0;
        public long lastTimestamp = 0;
        public boolean alerted = false;
    }

    private transient ValueState<OtpState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthOtpFallbackRateDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthOtpFallbackRateDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String authType = txn.getAuthType();
        if (authType == null) return;

        OtpState currentState = state.value();
        if (currentState == null) currentState = new OtpState();

        boolean isOtp = "O".equalsIgnoreCase(authType) || "OTP".equalsIgnoreCase(authType);
        
        currentState.recentAuthTypes.addLast(authType);
        if (isOtp) currentState.otpCount++;

        if (currentState.recentAuthTypes.size() > WINDOW_SIZE) {
            String removed = currentState.recentAuthTypes.removeFirst();
            if ("O".equalsIgnoreCase(removed) || "OTP".equalsIgnoreCase(removed)) {
                currentState.otpCount--;
            }
        }

        currentState.lastTimestamp = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        if (currentState.recentAuthTypes.size() >= 10) {
            double otpRatio = (double) currentState.otpCount / currentState.recentAuthTypes.size();
            
            if (otpRatio >= ALERT_THRESHOLD && !currentState.alerted) {
                emitFeature(ctx.getCurrentKey(), currentState, otpRatio, out);
                currentState.alerted = true;
            } else if (otpRatio < ALERT_THRESHOLD) {
                currentState.alerted = false; // Reset when ratio falls back to normal
            }
        }

        state.update(currentState);
    }

    private void emitFeature(String key, OtpState state, double ratio, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_otp_fallback_ratio_v1");
        feature.setFeatureType("Ratio");
        feature.setFeatureValue(ratio);
        feature.setWindowEnd(state.lastTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("otp_count", state.otpCount);
        comments.put("total_events", state.recentAuthTypes.size());
        comments.put("otp_ratio", ratio);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
