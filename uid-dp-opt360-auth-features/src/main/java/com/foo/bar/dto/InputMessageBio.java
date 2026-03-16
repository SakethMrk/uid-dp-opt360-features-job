package com.foo.bar.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@ToString
public class InputMessageBio {
    private long kafkaSourceTimestamp;
    private long kafkaSecondSourceTimestamp;
    private String topic;
    private String eventTimestamp;
    private String eventId;
    private String optId;
    private String enrRefId;
    private String authCode;
    private String aua;
    private String bodyPartTypeProbe;
    private String bioType;
    private String matchScore;
    private String probeMinutiae;
    private String probeSize;
    private String isLivenessCheckInvoked;
    private String livenessRequestInitiatedTime;
    private String livenessResponseReceivedTime;
    private String errorCode;
    private String isLive;
    private String modalityScore;
    private String deviceCode;
    private String islivenessEnabled;
    private String biolivenessStartTime;
    private String biolivenessEndtime;
    private String faceLivenessScore;
    private String imageSize;
    private String imageFormat;
    private String bioLivenessResponseTime;
    private String isFaceTemplate;
    private String faceLivenessClientRequestId;
    private String isAimlFaceMatcherAllowed;
    private String isMatched;
    private String faceMatchScore;
    private String faceMatchResponseTime;
    private String deepPrintRequestId;
    private String deepPrintFingerPositon;
    private String deepPrintIsMatched;
    private String deepPrintFIRMatchScore;
    private String deepPrintStartTime;
    private String deepPrintEndTime;
    private String deepPrintResponseTime;
}