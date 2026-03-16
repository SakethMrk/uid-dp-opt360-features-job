package com.foo.bar.functions;

import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.pipeline.Driver;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

public class WatermarkStrategies {

    public static WatermarkStrategy<InputMessageTxn> getWatermarkStrategy() {
        return WatermarkStrategy.<InputMessageTxn>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                .withTimestampAssigner((SerializableTimestampAssigner<InputMessageTxn>) (element, recordTimestamp) -> {
                    try {
                        String timeString = element.getReqDateTime();
                        if (timeString == null) {
                            return recordTimestamp;
                        }

                        return LocalDateTime.parse(timeString, Driver.Configurations.formatter)
                                .atZone(Driver.Configurations.istZone)
                                .toInstant()
                                .toEpochMilli();
                    } catch (DateTimeParseException e) {
                        return System.currentTimeMillis();
                    }
                });
    }
}