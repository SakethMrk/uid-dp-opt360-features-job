package com.foo.bar.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class OutMessage {

    @JsonProperty("entity_id")
    private String optId;

    @JsonProperty("feature_id")
    private String featureId;

    @JsonProperty("feature_name")
    private String feature;

    @JsonProperty("feature_version")
    private String featureVersion;

    @JsonProperty("feature_value")
    private Double featureValue;

    @JsonProperty("timestamp")
    private String timestampFormatted;

    @JsonProperty("comments")
    private String comments;

    @JsonIgnore
    private long windowStart;
    @JsonIgnore
    private long windowEnd;
    @JsonIgnore
    private String featureType;
    @JsonIgnore
    private long lastUpdatedTimestamp;
    @JsonIgnore
    private boolean skipComments = false;

    @Getter
    @Setter
    @NoArgsConstructor
    @ToString
    public static class OutMessageDeviceChange{

        private String optId;
        private long lastTimestampPreviousDevice;
        private long previousDeviceTxns;
        private String previousDevice;
        private String nextDevice;
        private String currAuthType;
        private long lastUpdatedTimestamp;
    }
}