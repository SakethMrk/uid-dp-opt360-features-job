package com.foo.bar.functions;

import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

//need key by on authtype as well
public class FeatureAuthGap extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    public static class GapState {
        public long lastEventTimestamp = -1, lastFailedEventTimestamp = -1, lastSuccessEventTimestamp = -1;
    }

    private transient ValueState<GapState> state;

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);
        
        StateDescriptors.featureAuthGapDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthGapDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {

        GapState currentState = state.value();
        String result = txn.getAuthResult();

        if (currentState == null)
            currentState = new GapState();

        long currentTimestamp = ctx.timestamp();

        if (currentTimestamp <= 0)
            currentTimestamp = System.currentTimeMillis();

        if (currentState.lastEventTimestamp != -1) {
            long gapMillis = currentTimestamp - currentState.lastEventTimestamp;

            if (gapMillis < 0)
                return;

            emitFeature(out, currentState.lastEventTimestamp, currentTimestamp, ctx.getCurrentKey(), "auth_txn_gap_sec");

            if("N".equalsIgnoreCase(result) && currentState.lastFailedEventTimestamp != -1)
                emitFeature(out, currentState.lastFailedEventTimestamp, currentTimestamp, ctx.getCurrentKey(), "auth_txn_failure_gap_sec");
            else if("Y".equalsIgnoreCase(result) && currentState.lastSuccessEventTimestamp != -1)
                emitFeature(out, currentState.lastSuccessEventTimestamp, currentTimestamp, ctx.getCurrentKey(), "auth_txn_success_gap_sec");
        }

        currentState.lastEventTimestamp = currentTimestamp;

        if ("N".equalsIgnoreCase(result))
            currentState.lastFailedEventTimestamp = currentTimestamp;
        else if("Y".equalsIgnoreCase(result))
            currentState.lastSuccessEventTimestamp = currentTimestamp;

        state.update(currentState);
    }

    private void emitFeature(Collector<OutMessage> out, long last, long current, String key, String featureName) {

        OutMessage feature = new OutMessage();

        feature.setOptId(key);
        feature.setFeature(featureName);
        feature.setFeatureType("Duration (seconds)");
        feature.setWindowStart(last);
        feature.setWindowEnd(current);
        feature.setFeatureValue((double) (current-last)/1000);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        out.collect(feature);
    }
}