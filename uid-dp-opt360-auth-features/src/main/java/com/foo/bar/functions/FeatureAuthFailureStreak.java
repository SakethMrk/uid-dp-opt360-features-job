package com.foo.bar.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import com.foo.bar.pipeline.Driver;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

@Slf4j
public class FeatureAuthFailureStreak extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static class FailureState{
        public int streakCount = 0;
        public long firstFailureTimestamp = 0;
        public long lastFailureTimestamp = 0;
        public String initialAuthCode = "";
        public String finalAuthCode = "";
        public Set<String> authCodes = new LinkedHashSet<>();
        public Set<String> deviceCodes = new LinkedHashSet<>();
        public String modelId = "";

        public long lastProcessedTimestamp = 0;
    }
    private transient ValueState<FailureState> State;
    private transient MapState<String,Void> dedupState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthFailureStreakDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        State = getRuntimeContext().getState(StateDescriptors.featureAuthFailureStreakDescriptor);

        MapStateDescriptor<String,Void> mapStateDescriptor = new MapStateDescriptor<>("dedupMap",String.class,Void.class);
        dedupState = getRuntimeContext().getMapState(mapStateDescriptor);
    }
    @Override
    public void processElement(InputMessageTxn inputMessageTxn, KeyedProcessFunction<String, InputMessageTxn, OutMessage>.Context context, Collector<OutMessage> collector) throws Exception {
        String currentAuthcode = inputMessageTxn.getAuthCode();
        if (currentAuthcode == null || currentAuthcode.isEmpty()) return;
        
        if(dedupState.contains(inputMessageTxn.getAuthCode())){
            return;
        }
        FailureState currentState = State.value();
        if(currentState == null){
            currentState = new FailureState();
        }
        long currentTimeStamp = inputMessageTxn.getKafkaSourceTimestamp();
        if(currentTimeStamp < 0){
            currentTimeStamp = System.currentTimeMillis();
        }

        String currentDeviceCode = inputMessageTxn.getDeviceCode();
        String authResult = inputMessageTxn.getAuthResult();
        if(authResult.equalsIgnoreCase("N")){
            if(currentAuthcode!=null && !currentState.authCodes.contains(currentAuthcode)){
                if(currentState.streakCount==0){
                    currentState.firstFailureTimestamp = currentTimeStamp;
                    currentState.initialAuthCode = currentAuthcode;
                    currentState.modelId = inputMessageTxn.getModelId();
                }
                if(currentDeviceCode!=null && !currentState.deviceCodes.contains(currentDeviceCode)){
                    currentState.deviceCodes.add(currentDeviceCode);
                }
                currentState.authCodes.add(currentAuthcode);
                currentState.streakCount = currentState.authCodes.size();
                currentState.lastFailureTimestamp = currentTimeStamp;
                currentState.finalAuthCode = currentAuthcode;
            }
            State.update(currentState);
        }
        else{
            if(currentState.streakCount>0) {
                log.info("Failure Streak broken for opt_id : {}", context.getCurrentKey());
                emitFeature(context.getCurrentKey(), currentState, collector);
                State.clear();
            }
        }
        dedupState.put(inputMessageTxn.getAuthCode(),null);
    }
    private void emitFeature(String key,FailureState failureState,Collector<OutMessage> out){
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_failure_streak");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) failureState.streakCount);
        feature.setWindowEnd(failureState.lastFailureTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String,Object> comments = new LinkedHashMap<>();
        comments.put("auth_code_initial", failureState.initialAuthCode);
        comments.put("auth_code_initial_timestamp", timestampToLocalDateTime(failureState.firstFailureTimestamp).toString());
        comments.put("auth_code_final", failureState.finalAuthCode);
        comments.put("auth_code_final_timestamp", timestampToLocalDateTime(failureState.lastFailureTimestamp).toString());
        comments.put("device_codes", new ArrayList<>(failureState.deviceCodes));
        comments.put("model_id", failureState.modelId);
        comments.put("streak_count", failureState.streakCount);

        try {
            String jsonString = objectMapper.writeValueAsString(comments);
            String finalString = jsonString.replace("\"", "'");
            feature.setComments(finalString);
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        feature.setSkipComments(true);
        out.collect(feature);
    }
    public static LocalDateTime timestampToLocalDateTime(long timestamp){
        return Instant.ofEpochMilli(timestamp).atZone(Driver.Configurations.istZone).toLocalDateTime();
    }
    
}
