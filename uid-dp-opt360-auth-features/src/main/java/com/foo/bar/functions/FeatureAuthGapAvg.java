package com.foo.bar.functions;

import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.LinkedList;

//consider night time
public class FeatureAuthGapAvg extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    public static class GapState {
        public long lastEventTimestamp = -1, eventCounter = 0;
        public LinkedList<Long> gapBuffer = new LinkedList<>();
    }

    private transient ValueState<GapState> state;

    @Override
    public void open(Configuration parameters) {
        
        StateDescriptors.featureAuthGapAvgDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthGapAvgDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {

        GapState currentState = state.value();

        if (currentState == null)
            currentState = new GapState();

        long currentTimestamp = ctx.timestamp();

        if (currentTimestamp <= 0)
            currentTimestamp = System.currentTimeMillis();

        if (currentState.lastEventTimestamp != -1) {
            long gapMillis = currentTimestamp - currentState.lastEventTimestamp;

            if (gapMillis < 0)
                gapMillis = 0;

            currentState.gapBuffer.addLast(gapMillis / 1000);

            if (currentState.gapBuffer.size() > 5)
                currentState.gapBuffer.removeFirst();

            currentState.eventCounter++;

            if (currentState.eventCounter >= 5) {
                emitFeature(ctx, out, currentState.gapBuffer, ctx.getCurrentKey());
                currentState.eventCounter = 0;
            }
        }

        currentState.lastEventTimestamp = currentTimestamp;
        state.update(currentState);
    }

    private void emitFeature(Context ctx, Collector<OutMessage> out, LinkedList<Long> gaps, String key) {

        if (gaps.isEmpty())
            return;

        double sum = 0;

        for (Long gap : gaps)
            sum += gap;

        double avgGap = sum / gaps.size();

        OutMessage feature = new OutMessage();

        feature.setOptId(key);
        feature.setFeature("auth_txn_gap_sec_avg");
        feature.setFeatureType("");
        feature.setWindowStart(ctx.timestamp());
        feature.setWindowEnd(ctx.timestamp());
        feature.setFeatureValue(avgGap);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        out.collect(feature);
    }
}