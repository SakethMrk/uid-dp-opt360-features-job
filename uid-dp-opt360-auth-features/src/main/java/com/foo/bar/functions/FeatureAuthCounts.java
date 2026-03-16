package com.foo.bar.functions;

import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.dto.OutMessage;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class FeatureAuthCounts {

    public static class CountAccumulator {
        public long totalCount = 0, failureCount = 0, successCount = 0;
    }

    public static class CountAggregator implements AggregateFunction<InputMessageTxn, CountAccumulator, CountAccumulator> {

        @Override
        public CountAccumulator createAccumulator() {
            return new CountAccumulator();
        }

        @Override
        public CountAccumulator add(InputMessageTxn txn, CountAccumulator acc) {

            if(txn.getAuthResult() != null)
                acc.totalCount++;

            if (txn.getAuthResult() != null && "N".equalsIgnoreCase(txn.getAuthResult()))
                acc.failureCount++;

            if (txn.getAuthResult() != null && "Y".equalsIgnoreCase(txn.getAuthResult()))
                acc.successCount++;

            return acc;
        }

        @Override
        public CountAccumulator getResult(CountAccumulator acc) {
            return acc;
        }

        @Override
        public CountAccumulator merge(CountAccumulator a, CountAccumulator b) {
            CountAccumulator merged = new CountAccumulator();
            merged.totalCount = a.totalCount + b.totalCount;
            merged.failureCount = a.failureCount + b.failureCount;
            return merged;
        }
    }

    public static class WindowResultFunction extends ProcessWindowFunction<CountAccumulator, OutMessage, String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<CountAccumulator> accs, Collector<OutMessage> out) {

            CountAccumulator finalCounts = accs.iterator().next();

            long timestamp = System.currentTimeMillis();

            OutMessage msg = new OutMessage();
            msg.setOptId(key);
            msg.setFeature("auth_txn_count");
            msg.setFeatureType("Count");
            msg.setWindowStart(context.window().getStart());
            msg.setWindowEnd(context.window().getEnd());
            msg.setFeatureValue((double) finalCounts.totalCount);
            msg.setLastUpdatedTimestamp(timestamp);

            out.collect(msg);

            msg.setFeature("auth_txn_failure_count");
            msg.setFeatureValue((double) finalCounts.failureCount);

            out.collect(msg);

            msg.setFeature("auth_txn_success_count");
            msg.setFeatureValue((double) finalCounts.successCount);

            out.collect(msg);
        }
    }
}