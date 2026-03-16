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
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public class FeatureAuthOddHourRatio extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {
    
    public static class OddHourState {
        public long currentCycleStartEpochMillis, totalTxns, oddHourTxns;
        public double highestEmittedRatio;
    }
    
    private transient ValueState<OddHourState> State;
    private static final double[] THRESHOLDS = {0.20, 0.50, 0.80, 1};

    @Override
    public void open(Configuration parameters) {
        
        StateDescriptors.featureAuthOddHourRatioDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        State = getRuntimeContext().getState(StateDescriptors.featureAuthOddHourRatioDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        
        long currentTimestamp = ctx.timestamp();
        
        if (currentTimestamp <= 0) 
            currentTimestamp = System.currentTimeMillis();
        
        ZonedDateTime eventTimeIst = Instant.ofEpochMilli(currentTimestamp).atZone(Driver.Configurations.istZone);
        
        ZonedDateTime cycleStartTime;
        
        if (eventTimeIst.getHour() < 8)
            cycleStartTime = eventTimeIst.truncatedTo(ChronoUnit.DAYS).minusDays(1).withHour(8);
        else
            cycleStartTime = eventTimeIst.truncatedTo(ChronoUnit.DAYS).withHour(8);
        
        long cycleStartMillis = cycleStartTime.toInstant().toEpochMilli();
        
        OddHourState state = State.value();
        
        if (state == null) {
            state = new OddHourState();
            state.currentCycleStartEpochMillis = cycleStartMillis;
        }
        
        if (cycleStartMillis > state.currentCycleStartEpochMillis) {
            state.currentCycleStartEpochMillis = cycleStartMillis;
            state.totalTxns = 0;
            state.oddHourTxns = 0;
            state.highestEmittedRatio = 0.0;
        } 
        else if (cycleStartMillis < state.currentCycleStartEpochMillis) 
            return;

        state.totalTxns++;

        int hour = eventTimeIst.getHour();

        if (hour >= 20 || hour < 8)
            state.oddHourTxns++;

        if (state.totalTxns >= 10) {

            double currentRatio = (double) state.oddHourTxns / state.totalTxns;
            double nextThreshold = getNextThreshold(state.highestEmittedRatio);

            if (currentRatio >= nextThreshold && nextThreshold > 0) {

                long windowStart = cycleStartTime.withHour(20).toInstant().toEpochMilli();
                long windowEnd = cycleStartTime.plusDays(1).withHour(8).toInstant().toEpochMilli();

                emitFeature(ctx, out, currentRatio, windowStart, windowEnd);
                state.highestEmittedRatio = currentRatio;
            }
        }

        State.update(state);
    }

    private double getNextThreshold(double currentLevel) {
        
        for (double threshold : THRESHOLDS) {
            if (currentLevel < threshold) 
                return threshold;
        }
        return -1.0;
    }

    private void emitFeature(Context ctx, Collector<OutMessage> out, double value, long start, long end) {
        
        OutMessage feature = new OutMessage();
        
        feature.setOptId(ctx.getCurrentKey());
        feature.setFeature("auth_txn_oddhour_ratio");
        feature.setFeatureType("");
        feature.setWindowStart(start); 
        feature.setWindowEnd(end);     
        feature.setFeatureValue(value);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());
        
        out.collect(feature);
    }
}