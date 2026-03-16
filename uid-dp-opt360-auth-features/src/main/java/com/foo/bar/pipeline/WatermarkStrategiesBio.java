package com.foo.bar.pipeline;

import com.foo.bar.dto.InputMessageBio;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

public class WatermarkStrategiesBio {

    public static WatermarkStrategy<InputMessageBio> getWatermarkStrategy() {
        return WatermarkStrategy
                .<InputMessageBio>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new BioTimestampAssigner());
    }

    public static class BioTimestampAssigner implements SerializableTimestampAssigner<InputMessageBio> {
        @Override
        public long extractTimestamp(InputMessageBio element, long recordTimestamp) {
            if (element != null && element.getEventTimestamp() != null && !element.getEventTimestamp().isEmpty() && !element.getEventTimestamp().equalsIgnoreCase("null")) {
                try {
                    return Long.parseLong(element.getEventTimestamp());
                } catch (NumberFormatException e) {
                    return recordTimestamp;
                }
            }
            return recordTimestamp;
        }
    }
}
