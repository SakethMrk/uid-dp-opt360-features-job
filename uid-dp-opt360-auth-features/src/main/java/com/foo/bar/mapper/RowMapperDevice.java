package com.foo.bar.mapper;

import com.foo.bar.dto.OutMessage;
import com.foo.bar.pipeline.Driver;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import java.time.Instant;
import java.time.LocalDateTime;

public class RowMapperDevice implements MapFunction<OutMessage.OutMessageDeviceChange, Row> {

    @Override
    public Row map(OutMessage.OutMessageDeviceChange value) {

        Row row = Row.of(
                value.getOptId(),
                timestampToLocalDateTime(value.getLastTimestampPreviousDevice()),
                value.getPreviousDeviceTxns(),
                value.getPreviousDevice(),
                value.getNextDevice(),
                value.getCurrAuthType(),
                timestampToLocalDateTime(value.getLastUpdatedTimestamp())
        );

        row.setKind(RowKind.INSERT);
        return row;
    }

    public static LocalDateTime timestampToLocalDateTime(long timestamp){
        return Instant.ofEpochMilli(timestamp).atZone(Driver.Configurations.istZone).toLocalDateTime();
    }
}