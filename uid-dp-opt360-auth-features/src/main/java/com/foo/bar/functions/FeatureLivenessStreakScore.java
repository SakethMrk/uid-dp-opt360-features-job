package com.foo.bar.functions;

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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

public class FeatureLivenessStreakScore extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    public static class StreakState{
        public int streakCount = 0;
        public long firstFailureTimestamp = 0;
        public long lastFailureTimestamp = 0;
        public String firstFailureAuthCode = "";
        public String lastFailureAuthCode = "";
        public Set<String> deviceCodes = new LinkedHashSet<>();
        public String modelId = "";
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
    private void emitFeature(String key, StreakState streakState,Collector<OutMessage> out){
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_liveness_failure_streak_live_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double)streakState.streakCount);
        feature.setWindowEnd(streakState.lastFailureTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        String firstAuthCode = streakState.firstFailureAuthCode;
        long firstFailureTimestamp = streakState.firstFailureTimestamp;
        String lastFailureAuthCode = streakState.lastFailureAuthCode;
        long lastFailureTimestamp = streakState.lastFailureTimestamp;
        String modelId = streakState.modelId;

        StringBuilder deviceCodesList = new StringBuilder();
        for(String str: streakState.deviceCodes){
            if(deviceCodesList.length()>0){
                deviceCodesList.append(", ");
            }
            deviceCodesList.append(str);
        }
        StringBuilder sb = new StringBuilder();
              sb.append(" auth_code_initial:").append(firstAuthCode)
                .append(",\nauth_code_initial_timestamp:").append(timestampToLocalDateTime(firstFailureTimestamp))
                .append(",\nauth_code_final:").append(lastFailureAuthCode)
                .append(",\nauth_code_final_timestamp:").append(timestampToLocalDateTime(lastFailureTimestamp))
                .append(",\ndevice_codes:").append(deviceCodesList)
                .append(",\nmodel_id:").append(modelId);

        feature.setComments(sb.toString());
        feature.setSkipComments(true);
        out.collect(feature);
    }
    public static LocalDateTime timestampToLocalDateTime(long timestamp){
        return Instant.ofEpochMilli(timestamp).atZone(Driver.Configurations.istZone).toLocalDateTime();
    }
}
