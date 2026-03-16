package com.foo.bar.mapper;

import com.foo.bar.dto.OutMessage;
import com.foo.bar.pipeline.Driver;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.*;

import java.time.Instant;
import java.time.LocalDateTime;

public class RowMapper implements MapFunction<OutMessage, Row> {

    @Override
    public Row map(OutMessage value) {

        long timestampMillis = value.getWindowEnd() > 0 ? value.getWindowEnd() : value.getLastUpdatedTimestamp();
        String comments = value.getComments();
        if (comments!=null) {
            comments = comments.replace("\n", "");
        }
        Row row = Row.of(
                value.getOptId(),
                value.getFeatureId(),
                value.getFeature(),
                value.getFeatureVersion(),
                value.getFeatureValue(),
                timestampToLocalDateTime(timestampMillis),
                comments
        );
        row.setKind(RowKind.INSERT);
        return row;
    }
    public static LocalDateTime timestampToLocalDateTime(long timestamp){
        return Instant.ofEpochMilli(timestamp).atZone(Driver.Configurations.istZone).toLocalDateTime();
    }
}