package com.foo.bar.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class InputMessageTxn {
    private long kafkaSourceTimestamp;
    private long kafkaSecondSourceTimestamp;
    private String topic;
    private String eventId;
    private String authCode;
    private String eventTimestamp;
    private String reqDateTime;
    private String optId;
    private String enrolmentReferenceId;
    private String aua;
    private String sa;
    private String asa;
    private String authResult;
    private String errorCode;
    private String subErrorCode;
    private String authDuration;
    private String authType;
    private String registeredDeviceSoftwareId;
    private String registeredDeviceSoftwareVersion;
    private String deviceProviderId;
    private String modelId;
    private String deviceCode;
    private String certExpiryDate;
    private String faceMatchScore;
    private String faceMatchType;
    private String faceSdkVersion;
    private String fingerMatchThreshold;
    private String fmrSDKVersion;
    private String fingerMatchScore;
    private String otpIdentifier;
    private String serverId;

    private String locationStateCode;
    private String locationDistrictCode;
    private String pidSize;
    private String residentStateCode;
    private String residentDistrictCode;
    private String fmrCount;
    private String firCount;
    private String faceUsed;
    private String fdc;
    private String idc;
    private String deviceMetaData;
    private String fingerFusionPerfomed;
    private String irisFusionPerfomed;
    private String faceFusionDone;
}