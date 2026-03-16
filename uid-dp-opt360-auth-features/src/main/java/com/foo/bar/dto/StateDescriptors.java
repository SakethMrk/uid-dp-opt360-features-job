package com.foo.bar.dto;

import com.foo.bar.functions.*;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class StateDescriptors {

    public static StateTtlConfig stateTtlFunction()
    {
        return StateTtlConfig.newBuilder(Time.hours(48))
            .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .cleanupFullSnapshot()
            .build();
    }
    public static StateTtlConfig stateTtlFunction1Day() {
        return StateTtlConfig.newBuilder(Time.hours(24))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();
    }

    public static ValueStateDescriptor<FeatureAuthGapAvg.GapState> featureAuthGapAvgDescriptor = new ValueStateDescriptor<>(
            "featureAuthGapAvgDescriptor",
            TypeInformation.of(new TypeHint<>() {})
    );

    public static ValueStateDescriptor<FeatureAuthGap.GapState> featureAuthGapDescriptor = new ValueStateDescriptor<>(
            "featureAuthGapDescriptor",
            TypeInformation.of(new TypeHint<>() {})
    );

    public static ValueStateDescriptor<FeatureAuthDeviceChange.DevState> featureAuthDeviceChangeDescriptor = new ValueStateDescriptor<>(
            "featureAuthDeviceChangeDescriptor",
            TypeInformation.of(new TypeHint<>() {})
    );

    public static ValueStateDescriptor<FeatureAuthRetrySession.RetryState> featureAuthRetrySessionDescriptor = new ValueStateDescriptor<>(
            "featureAuthRetrySessionDescriptor",
            TypeInformation.of(new TypeHint<>() {})
    );

    public static ValueStateDescriptor<FeatureAuthOddHourRatio.OddHourState> featureAuthOddHourRatioDescriptor = new ValueStateDescriptor<>(
            "featureAuthOddHourRatioDescriptor",
            TypeInformation.of(new TypeHint<>() {})
    );
    public static ValueStateDescriptor<FeatureLivenessStreakScore.StreakState> featureLivenessStreakDescriptor = new ValueStateDescriptor<>(
            "featureLivenessStreakDescriptor",
            TypeInformation.of(new TypeHint<FeatureLivenessStreakScore.StreakState>() {})
    );

    // --- NEW TXN FEATURES STATE DESCRIPTORS ---
    public static ValueStateDescriptor<FeatureAuthFailureRate.RateState> featureAuthFailureRateDescriptor = new ValueStateDescriptor<>(
            "featureAuthFailureRateDescriptor", TypeInformation.of(new TypeHint<FeatureAuthFailureRate.RateState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthOtpFallbackRatio.OtpState> featureAuthOtpFallbackRateDescriptor = new ValueStateDescriptor<>(
            "featureAuthOtpFallbackRateDescriptor", TypeInformation.of(new TypeHint<FeatureAuthOtpFallbackRatio.OtpState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthSuccessWithoutBio.SuccessState> featureAuthSuccessWithoutBioDescriptor = new ValueStateDescriptor<>(
            "featureAuthSuccessWithoutBioDescriptor", TypeInformation.of(new TypeHint<FeatureAuthSuccessWithoutBio.SuccessState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthVelocitySpike.BurstState> featureAuthVelocitySpikeDescriptor = new ValueStateDescriptor<>(
            "featureAuthVelocitySpikeDescriptor", TypeInformation.of(new TypeHint<FeatureAuthVelocitySpike.BurstState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthFailureBurst.BurstState> featureAuthFailureBurstDescriptor = new ValueStateDescriptor<>(
            "featureAuthFailureBurstDescriptor", TypeInformation.of(new TypeHint<FeatureAuthFailureBurst.BurstState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthGeoDrift.GeoState> featureAuthGeoDriftDescriptor = new ValueStateDescriptor<>(
            "featureAuthGeoDriftDescriptor", TypeInformation.of(new TypeHint<FeatureAuthGeoDrift.GeoState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthLocationResidentMismatch.MismatchState> featureAuthLocationResidentMismatchDescriptor = new ValueStateDescriptor<>(
            "featureAuthLocationResidentMismatchDescriptor", TypeInformation.of(new TypeHint<FeatureAuthLocationResidentMismatch.MismatchState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthDeviceDiversity.DeviceState> featureAuthDeviceDiversityDescriptor = new ValueStateDescriptor<>(
            "featureAuthDeviceDiversityDescriptor", TypeInformation.of(new TypeHint<FeatureAuthDeviceDiversity.DeviceState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthModelDiversity.ModelState> featureAuthModelDiversityDescriptor = new ValueStateDescriptor<>(
            "featureAuthModelDiversityDescriptor", TypeInformation.of(new TypeHint<FeatureAuthModelDiversity.ModelState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthRdSoftwareChange.SoftwareState> featureAuthRdSoftwareChangeDescriptor = new ValueStateDescriptor<>(
            "featureAuthRdSoftwareChangeDescriptor", TypeInformation.of(new TypeHint<FeatureAuthRdSoftwareChange.SoftwareState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthDeviceProviderSwitch.ProviderState> featureAuthDeviceProviderSwitchDescriptor = new ValueStateDescriptor<>(
            "featureAuthDeviceProviderSwitchDescriptor", TypeInformation.of(new TypeHint<FeatureAuthDeviceProviderSwitch.ProviderState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthFingerScoreDecline.ScoreState> featureAuthFingerScoreDeclineDescriptor = new ValueStateDescriptor<>(
            "featureAuthFingerScoreDeclineDescriptor", TypeInformation.of(new TypeHint<FeatureAuthFingerScoreDecline.ScoreState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthFaceScoreDecline.ScoreState> featureAuthFaceScoreDeclineDescriptor = new ValueStateDescriptor<>(
            "featureAuthFaceScoreDeclineDescriptor", TypeInformation.of(new TypeHint<FeatureAuthFaceScoreDecline.ScoreState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthFmrCountAnomaly.FmrState> featureAuthFmrCountAnomalyDescriptor = new ValueStateDescriptor<>(
            "featureAuthFmrCountAnomalyDescriptor", TypeInformation.of(new TypeHint<FeatureAuthFmrCountAnomaly.FmrState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthPidSizeAnomaly.PidState> featureAuthPidSizeAnomalyDescriptor = new ValueStateDescriptor<>(
            "featureAuthPidSizeAnomalyDescriptor", TypeInformation.of(new TypeHint<FeatureAuthPidSizeAnomaly.PidState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthTimeEntropy.EntropyState> featureAuthTimeEntropyDescriptor = new ValueStateDescriptor<>(
            "featureAuthTimeEntropyDescriptor", TypeInformation.of(new TypeHint<FeatureAuthTimeEntropy.EntropyState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthWeekendAnomaly.WeekendState> featureAuthWeekendAnomalyDescriptor = new ValueStateDescriptor<>(
            "featureAuthWeekendAnomalyDescriptor", TypeInformation.of(new TypeHint<FeatureAuthWeekendAnomaly.WeekendState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthAuaSaDiversity.AuaSaState> featureAuthAuaSaDiversityDescriptor = new ValueStateDescriptor<>(
            "featureAuthAuaSaDiversityDescriptor", TypeInformation.of(new TypeHint<FeatureAuthAuaSaDiversity.AuaSaState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthEnrolmentHammering.HammeringState> featureAuthEnrolmentHammeringDescriptor = new ValueStateDescriptor<>(
            "featureAuthEnrolmentHammeringDescriptor", TypeInformation.of(new TypeHint<FeatureAuthEnrolmentHammering.HammeringState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthDurationOutlier.DurationState> featureAuthDurationOutlierDescriptor = new ValueStateDescriptor<>(
            "featureAuthDurationOutlierDescriptor", TypeInformation.of(new TypeHint<FeatureAuthDurationOutlier.DurationState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthServerConcentration.ServerState> featureAuthServerConcentrationDescriptor = new ValueStateDescriptor<>(
            "featureAuthServerConcentrationDescriptor", TypeInformation.of(new TypeHint<FeatureAuthServerConcentration.ServerState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthCertExpiry.CertState> featureAuthCertExpiryDescriptor = new ValueStateDescriptor<>(
            "featureAuthCertExpiryDescriptor", TypeInformation.of(new TypeHint<FeatureAuthCertExpiry.CertState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthErrorCodeHotspot.ErrorState> featureAuthErrorCodeHotspotDescriptor = new ValueStateDescriptor<>(
            "featureAuthErrorCodeHotspotDescriptor", TypeInformation.of(new TypeHint<FeatureAuthErrorCodeHotspot.ErrorState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthModalitySwitch.ModalityState> featureAuthModalitySwitchDescriptor = new ValueStateDescriptor<>(
            "featureAuthModalitySwitchDescriptor", TypeInformation.of(new TypeHint<FeatureAuthModalitySwitch.ModalityState>() {})
    );
}