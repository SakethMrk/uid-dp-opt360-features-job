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
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class FeatureLivenessStreakScore extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static class StreakState{
        public int streakCount = 0;
        public long firstFailureTimestamp = 0;
        public long lastFailureTimestamp = 0;
        public String firstFailureAuthCode = "";
        public String lastFailureAuthCode = "";
        public Set<String> deviceCodes = new LinkedHashSet<>();
        public String modelId = "";
        public String lastProcessedEventId = "";
    }
    private transient ValueState<StreakState> State;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureLivenessStreakDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction1Day());
        State = getRuntimeContext().getState(StateDescriptors.featureLivenessStreakDescriptor);
    }
    @Override
    public void processElement(InputMessageTxn inputMessageTxn, KeyedProcessFunction<String, InputMessageTxn, OutMessage>.Context context, Collector<OutMessage> collector) throws Exception {
        StreakState currentState = State.value();
        if(currentState == null){
            currentState = new StreakState();
        }
        long currentTimestamp = context.timestamp();
        if(currentTimestamp < 0){
            currentTimestamp = System.currentTimeMillis();
        }

        String eventId = inputMessageTxn.getEventId();
        if (eventId != null && !eventId.isEmpty()) {
            if (eventId.equals(currentState.lastProcessedEventId)) {
                return; // Duplicate event dedup guard
            }
            currentState.lastProcessedEventId = eventId;
        }

        // Logical ordering guard for distinct events with identical timestamps
        if (currentState.streakCount > 0 && currentTimestamp <= currentState.lastFailureTimestamp) {
            currentTimestamp = currentState.lastFailureTimestamp + 1;
        }

        String subErrorCode = inputMessageTxn.getSubErrorCode();
        String currentDeviceCode = inputMessageTxn.getDeviceCode();
        if(subErrorCode!=null && subErrorCode.contains("300-3")){
            // 1st time
            if(currentState.streakCount==0) {
                currentState.firstFailureTimestamp = currentTimestamp;
                currentState.firstFailureAuthCode = inputMessageTxn.getAuthCode();
                currentState.modelId = inputMessageTxn.getModelId();
            }
            if(currentDeviceCode!=null && !currentDeviceCode.isEmpty()){
                currentState.deviceCodes.add(currentDeviceCode);
            }
                currentState.streakCount++;
                currentState.lastFailureTimestamp = currentTimestamp;
                currentState.lastFailureAuthCode = inputMessageTxn.getAuthCode();

                State.update(currentState);
            }
        // streak broken
        else{
            if(currentState.streakCount>0){
                emitFeature(context.getCurrentKey(),currentState,collector);
                State.clear();
            }
        }

    }
    private void emitFeature(String key, StreakState streakState, Collector<OutMessage> out){
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_liveness_failure_streak_live_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double)streakState.streakCount);
        feature.setWindowEnd(streakState.lastFailureTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> commentsMap = new LinkedHashMap<>();
        commentsMap.put("auth_code_initial", streakState.firstFailureAuthCode);
        commentsMap.put("auth_code_initial_timestamp", timestampToLocalDateTime(streakState.firstFailureTimestamp).toString());
        commentsMap.put("auth_code_final", streakState.lastFailureAuthCode);
        commentsMap.put("auth_code_final_timestamp", timestampToLocalDateTime(streakState.lastFailureTimestamp).toString());
        commentsMap.put("device_codes", new ArrayList<>(streakState.deviceCodes));
        commentsMap.put("model_id", streakState.modelId);
        commentsMap.put("streak_count", streakState.streakCount);

        try {
            feature.setComments(objectMapper.writeValueAsString(commentsMap));
        } catch (Exception e) {
            feature.setComments(commentsMap.toString());
        }
        feature.setSkipComments(true);
        out.collect(feature);
    }
    public static LocalDateTime timestampToLocalDateTime(long timestamp){
        return Instant.ofEpochMilli(timestamp).atZone(Driver.Configurations.istZone).toLocalDateTime();
    }
}
