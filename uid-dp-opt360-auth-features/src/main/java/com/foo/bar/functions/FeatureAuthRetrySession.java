package com.foo.bar.functions;

import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.dto.StateDescriptors;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

//key by authType
public class FeatureAuthRetrySession extends KeyedProcessFunction<String, InputMessageTxn, OutMessage> {
    
    public static class RetryState {
        public long firstFailureTimestamp;
        public int failureCount;
    }

    private transient ValueState<RetryState> State;
    private static final long SESSION_TIMEOUT_MS = 2 * 60 * 1000L;

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);

        StateDescriptors.featureAuthRetrySessionDescriptor.enableTimeToLive(StateDescriptors.stateTtlFunction());
        State = getRuntimeContext().getState(StateDescriptors.featureAuthRetrySessionDescriptor);
    }

    @Override
    public void processElement(InputMessageTxn txn, Context ctx, Collector<OutMessage> out) throws Exception {

        String result = txn.getAuthResult();

        if (result == null)
            return;

        long currentTimestamp = ctx.timestamp();

        if (currentTimestamp <= 0)
            currentTimestamp = System.currentTimeMillis();

        RetryState state = State.value();

        if ("N".equalsIgnoreCase(result)) {

            if (state == null) {
                state = new RetryState();
                state.firstFailureTimestamp = currentTimestamp;
                state.failureCount = 1;
            }
            else {
                long gap = currentTimestamp - state.firstFailureTimestamp;

                if (gap >= 0 && gap <= SESSION_TIMEOUT_MS) {
                    state.failureCount++;
                }
                else if (gap < 0) {

                    state.firstFailureTimestamp = currentTimestamp;

                    if (Math.abs(gap) <= SESSION_TIMEOUT_MS)
                        state.failureCount++;
                    else
                        state.failureCount = 1;
                } else {
                    state.firstFailureTimestamp = currentTimestamp;
                    state.failureCount = 1;
                }
            }

            State.update(state);
        }
        else if ("Y".equalsIgnoreCase(result)) {

            if (state != null) {
                long duration = currentTimestamp - state.firstFailureTimestamp;

                if (duration >= 0 && duration <= SESSION_TIMEOUT_MS) {

                    long timestamp = System.currentTimeMillis();

                    OutMessage msg = new OutMessage();

                    msg.setOptId(ctx.getCurrentKey());
                    msg.setFeature("auth_retry_attempts");
                    msg.setFeatureType("Count");
                    msg.setWindowStart(state.firstFailureTimestamp);
                    msg.setWindowEnd(currentTimestamp);
                    msg.setFeatureValue((double) state.failureCount);
                    msg.setLastUpdatedTimestamp(timestamp);

                    out.collect(msg);

                    msg.setFeature("auth_retry_duration_sec");
                    msg.setFeatureType("Duration (seconds)");
                    msg.setFeatureValue((double) (duration / 1000));

                    out.collect(msg);
                }

                State.clear();
            }
        }
    }
}