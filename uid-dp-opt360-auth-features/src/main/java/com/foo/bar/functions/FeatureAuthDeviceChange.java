package com.foo.bar.functions;

import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;

//key by authType
public class FeatureAuthDeviceChange extends KeyedProcessFunction<String, InputMessageTxn, OutMessage.OutMessageDeviceChange> {

    public static class DevState {
        public long lastTimestampPreviousDevice = -1, eventsBeforeChange = -1;
        public String previousDevice = "";
    }

    private transient ValueState<DevState> state;

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);

        StateDescriptors.featureAuthDeviceChangeDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthDeviceChangeDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage.OutMessageDeviceChange> out) throws Exception {

        long currentTimestamp = ctx.timestamp();

        if (currentTimestamp <= 0)
            currentTimestamp = System.currentTimeMillis();

        DevState currentState = state.value();

        if (currentState == null)
            currentState = new DevState();

        String currDevice = txn.getDeviceCode();

        if (Objects.equals(currentState.previousDevice, ""))
        {
            currentState.previousDevice = currDevice;
            currentState.eventsBeforeChange = 1;
        }
        else if(Objects.equals(currentState.previousDevice, currDevice))
            ++currentState.eventsBeforeChange;
        else
        {
            emitFeature(out, ctx.getCurrentKey(), currentState.lastTimestampPreviousDevice,
                    currentState.eventsBeforeChange, currentState.previousDevice, currDevice, txn.getAuthType());

            currentState.previousDevice = currDevice;
            currentState.eventsBeforeChange = 1;
        }

        currentState.lastTimestampPreviousDevice = currentTimestamp;
        state.update(currentState);
    }

    private void emitFeature(Collector<OutMessage.OutMessageDeviceChange> out, String key, long prevTimestamp,
                             long eventCount, String prevDevice, String currDevice, String authType) {

        OutMessage.OutMessageDeviceChange feature = new OutMessage.OutMessageDeviceChange();

        feature.setOptId(key);
        feature.setLastTimestampPreviousDevice(prevTimestamp);
        feature.setPreviousDeviceTxns(eventCount);
        feature.setPreviousDevice(prevDevice);
        feature.setNextDevice(currDevice);
        feature.setCurrAuthType(authType);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        out.collect(feature);
    }
}