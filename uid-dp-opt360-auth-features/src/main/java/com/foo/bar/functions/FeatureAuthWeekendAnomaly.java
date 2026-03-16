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

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

public class FeatureAuthWeekendAnomaly extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final long WEEK_MS = 7 * 24 * 60 * 60 * 1000L;

    public static class WeekendState {
        public int weekdayCount = 0;
        public int weekendCount = 0;
        public long cycleStart = 0;
        public long lastTimestamp = 0;
        public boolean alerted = false;
    }

    private transient ValueState<WeekendState> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StateDescriptors.featureAuthWeekendAnomalyDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        state = getRuntimeContext().getState(StateDescriptors.featureAuthWeekendAnomalyDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {
        long currentTs = ctx.timestamp() > 0 ? ctx.timestamp() : System.currentTimeMillis();
        ZonedDateTime zdt = Instant.ofEpochMilli(currentTs).atZone(Driver.Configurations.istZone);
        
        WeekendState currentState = state.value();
        if (currentState == null) {
            currentState = new WeekendState();
             currentState.cycleStart = currentTs;
        } else if (currentTs - currentState.cycleStart > WEEK_MS) {
            currentState = new WeekendState();
            currentState.cycleStart = currentTs;
        }

        DayOfWeek dow = zdt.getDayOfWeek();
        boolean isWeekend = dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY;

        if (isWeekend) {
            currentState.weekendCount++;
        } else {
            currentState.weekdayCount++;
        }

        currentState.lastTimestamp = currentTs;

        int total = currentState.weekdayCount + currentState.weekendCount;
        if (total >= 20) {
            double weekendRatio = (double) currentState.weekendCount / total;
            
            // Anomalous if predominantly weekend activity
            if (weekendRatio > 0.50 && !currentState.alerted) {
                emitFeature(ctx.getCurrentKey(), weekendRatio, currentState, out);
                currentState.alerted = true;
            } else if (weekendRatio <= 0.50) {
                currentState.alerted = false;
            }
        }

        state.update(currentState);
    }

    private void emitFeature(String key, double ratio, WeekendState state, Collector<OutMessage> out) {
        OutMessage feature = new OutMessage();
        feature.setOptId(key);
        feature.setFeature("auth_weekend_anomaly_v1");
        feature.setFeatureType("Ratio");
        feature.setFeatureValue(ratio);
        feature.setWindowStart(state.cycleStart);
        feature.setWindowEnd(state.lastTimestamp);
        feature.setLastUpdatedTimestamp(System.currentTimeMillis());

        Map<String, Object> comments = new LinkedHashMap<>();
        comments.put("weekend_ratio", ratio);
        comments.put("weekend_count", state.weekendCount);
        comments.put("weekday_count", state.weekdayCount);

        try {
            feature.setComments(objectMapper.writeValueAsString(comments));
        } catch (Exception e) {
            feature.setComments(comments.toString());
        }
        
        feature.setSkipComments(true);
        out.collect(feature);
    }
}
