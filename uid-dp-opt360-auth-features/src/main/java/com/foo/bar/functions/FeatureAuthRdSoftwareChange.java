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
import java.util.Map;

public class FeatureAuthRdSoftwareChange extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static class SoftwareState {
        public String lastSoftwareId = "";
        public String lastSoftwareVersion = "";
        public int changeCount = 0;
        public long lastTimestamp = 0;
    }

    private transient ValueState<SoftwareState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthRdSoftwareChangeDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthRdSoftwareChangeDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        String swId = txn.getRegisteredDeviceSoftwareId();
        String swVersion = txn.getRegisteredDeviceSoftwareVersion();
        
        if (swId == null || swVersion == null || swId.trim().isEmpty() || swVersion.trim().isEmpty()) return;

        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();

        SoftwareState currentState = state.value();
        if (currentState == null) {
            currentState = new SoftwareState();
            currentState.lastSoftwareId = swId;
            currentState.lastSoftwareVersion = swVersion;
            currentState.lastTimestamp = currentTs;
            state.update(currentState);
            return;
        }

        boolean idChanged = !swId.equals(currentState.lastSoftwareId);
        boolean versionChanged = !swVersion.equals(currentState.lastSoftwareVersion);

        if (idChanged || versionChanged) {
            currentState.changeCount++;
            emitFeature(ctx.getCurrentKey(), currentState.lastSoftwareId, currentState.lastSoftwareVersion, swId, swVersion, currentState.changeCount, currentTs, currentState.lastTimestamp, out);
            
            currentState.lastSoftwareId = swId;
            currentState.lastSoftwareVersion = swVersion;
        }

        currentState.lastTimestamp = currentTs;
        state.update(currentState);
    }

    private void emitFeature(String key, String oldId, String oldVer, String newId, String newVer, int changeCount, long currentTs, long lastTs, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_rd_software_change_v1");
        feature.setFeatureType("Count");
        feature.setFeatureValue((double) changeCount);
        feature.setWindowStart(lastTs);
        feature.setWindowEnd(currentTs);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("total_changes", changeCount);
        comments.put("previous_id", oldId);
        comments.put("previous_version", oldVer);
        comments.put("new_id", newId);
        comments.put("new_version", newVer);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
