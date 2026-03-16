package com.foo.bar.mapper;

import com.foo.bar.dto.OutMessage;
import com.foo.bar.pipeline.Driver;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.Instant;
import java.time.LocalDateTime;

public class CommentsMapper implements MapFunction<OutMessage, OutMessage> {

    private static final int FEATURE_VERSION = 1;

    @Override
    public OutMessage map(OutMessage value) {

        StringBuilder featureId = new StringBuilder(value.getFeature());
        if(value.getWindowStart() > 0 && value.getWindowEnd() > 0) {
            long duration = (value.getWindowEnd() - value.getWindowStart()) / 60000;
            if(duration > 0 && value.getFeature().contains("count")) {
                featureId.append("_").append(duration).append("m");
            }
        }
        featureId.append("_v").append(FEATURE_VERSION);
        value.setFeatureId(featureId.toString());
        value.setFeatureVersion(String.valueOf(FEATURE_VERSION));


        long timestampMillis = value.getWindowEnd() > 0 ? value.getWindowEnd() : value.getLastUpdatedTimestamp();
        value.setTimestampFormatted(timestampToLocalDateTime(timestampMillis).toString());

        if (!value.isSkipComments() && (value.getComments() == null || value.getComments().isEmpty())) {
            StringBuilder sb = new StringBuilder();
            if (value.getWindowStart() > 0 && value.getWindowEnd() > 0) {
                sb.append("Window: [")
                        .append(timestampToLocalDateTime(value.getWindowStart()))
                        .append(" - ")
                        .append(timestampToLocalDateTime(value.getWindowEnd()))
                        .append("]");
            }
            if (value.getLastUpdatedTimestamp() > 0) {
                sb.append(", LastUpdated: ").append(timestampToLocalDateTime(value.getLastUpdatedTimestamp()));
            }
            value.setComments(sb.toString());
        }

        return value;
    }

    public static LocalDateTime timestampToLocalDateTime(long timestamp) {
        return Instant.ofEpochMilli(timestamp).atZone(Driver.Configurations.istZone).toLocalDateTime();
    }
}