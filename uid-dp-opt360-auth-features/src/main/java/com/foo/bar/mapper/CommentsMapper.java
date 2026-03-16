package com.foo.bar.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.pipeline.Driver;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

public class CommentsMapper implements MapFunction<OutMessage, OutMessage> {

    private static final int FEATURE_VERSION = 1;
    private static final ObjectMapper objectMapper = new ObjectMapper();

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
            Map<String, Object> map = new LinkedHashMap<>();
            if (value.getWindowStart() > 0 && value.getWindowEnd() > 0) {
                map.put("window_start", timestampToLocalDateTime(value.getWindowStart()).toString());
                map.put("window_end", timestampToLocalDateTime(value.getWindowEnd()).toString());
            }
            if (value.getLastUpdatedTimestamp() > 0) {
                map.put("last_updated", timestampToLocalDateTime(value.getLastUpdatedTimestamp()).toString());
            }
            
            if (!map.isEmpty()) {
                try {
                    value.setComments(objectMapper.writeValueAsString(map));
                } catch (Exception e) {
                    value.setComments(map.toString());
                }
            }
        }

        return value;
    }

    public static LocalDateTime timestampToLocalDateTime(long timestamp) {
        return Instant.ofEpochMilli(timestamp).atZone(Driver.Configurations.istZone).toLocalDateTime();
    }
}