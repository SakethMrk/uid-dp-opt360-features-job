package com.foo.bar.dto;

import com.foo.bar.functions.*;
import com.foo.bar.functions.bio.*;
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
    public static ValueStateDescriptor<FeatureAuthGhostUptime.UptimeState> featureAuthGhostUptimeDescriptor = new ValueStateDescriptor<>(
            "featureAuthGhostUptimeDescriptor", TypeInformation.of(new TypeHint<FeatureAuthGhostUptime.UptimeState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthCrossBorderVelocity.GeoVelocityState> featureAuthCrossBorderVelocityDescriptor = new ValueStateDescriptor<>(
            "featureAuthCrossBorderVelocityDescriptor", TypeInformation.of(new TypeHint<FeatureAuthCrossBorderVelocity.GeoVelocityState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthConcurrentAuaBurst.ConcurrentAuaState> featureAuthConcurrentAuaBurstDescriptor = new ValueStateDescriptor<>(
            "featureAuthConcurrentAuaBurstDescriptor", TypeInformation.of(new TypeHint<FeatureAuthConcurrentAuaBurst.ConcurrentAuaState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthBiometricReplay.ReplayState> featureAuthBiometricReplayDescriptor = new ValueStateDescriptor<>(
            "featureAuthBiometricReplayDescriptor", TypeInformation.of(new TypeHint<FeatureAuthBiometricReplay.ReplayState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthHighRiskTimeWindow.HighRiskState> featureAuthHighRiskTimeWindowDescriptor = new ValueStateDescriptor<>(
            "featureAuthHighRiskTimeWindowDescriptor", TypeInformation.of(new TypeHint<FeatureAuthHighRiskTimeWindow.HighRiskState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthFailureRecoverySpeed.RecoveryState> featureAuthFailureRecoverySpeedDescriptor = new ValueStateDescriptor<>(
            "featureAuthFailureRecoverySpeedDescriptor", TypeInformation.of(new TypeHint<FeatureAuthFailureRecoverySpeed.RecoveryState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthRapidDeviceSwitch.RapidSwitchState> featureAuthRapidDeviceSwitchDescriptor = new ValueStateDescriptor<>(
            "featureAuthRapidDeviceSwitchDescriptor", TypeInformation.of(new TypeHint<FeatureAuthRapidDeviceSwitch.RapidSwitchState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthMultiErrorSpray.SprayState> featureAuthMultiErrorSprayDescriptor = new ValueStateDescriptor<>(
            "featureAuthMultiErrorSprayDescriptor", TypeInformation.of(new TypeHint<FeatureAuthMultiErrorSpray.SprayState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthNightSurge.SurgeState> featureAuthNightSurgeDescriptor = new ValueStateDescriptor<>(
            "featureAuthNightSurgeDescriptor", TypeInformation.of(new TypeHint<FeatureAuthNightSurge.SurgeState>() {})
    );
    public static ValueStateDescriptor<FeatureAuthConsecutiveIdenticalEvents.IdenticalState> featureAuthConsecutiveIdenticalEventsDescriptor = new ValueStateDescriptor<>(
            "featureAuthConsecutiveIdenticalEventsDescriptor", TypeInformation.of(new TypeHint<FeatureAuthConsecutiveIdenticalEvents.IdenticalState>() {})
    );
    public static ValueStateDescriptor<FeatureBioMatchScoreTrend.ScoreState> featureBioMatchScoreTrendDescriptor = new ValueStateDescriptor<>(
            "featureBioMatchScoreTrendDescriptor", TypeInformation.of(new TypeHint<FeatureBioMatchScoreTrend.ScoreState>() {})
    );
    public static ValueStateDescriptor<FeatureBioLivenessFailureStreak.StreakState> featureBioLivenessFailureStreakDescriptor = new ValueStateDescriptor<>(
            "featureBioLivenessFailureStreakDescriptor", TypeInformation.of(new TypeHint<FeatureBioLivenessFailureStreak.StreakState>() {})
    );
    public static ValueStateDescriptor<FeatureBioResponseTimeAnomaly.TimeState> featureBioResponseTimeAnomalyDescriptor = new ValueStateDescriptor<>(
            "featureBioResponseTimeAnomalyDescriptor", TypeInformation.of(new TypeHint<FeatureBioResponseTimeAnomaly.TimeState>() {})
    );
    public static ValueStateDescriptor<FeatureBioDeepPrintAnomaly.DeepPrintState> featureBioDeepPrintAnomalyDescriptor = new ValueStateDescriptor<>(
            "featureBioDeepPrintAnomalyDescriptor", TypeInformation.of(new TypeHint<FeatureBioDeepPrintAnomaly.DeepPrintState>() {})
    );

    // --- NEW BIO FEATURES ---
    public static ValueStateDescriptor<FeatureBioCaptureTimeSpoof.CaptureState> featureBioCaptureTimeSpoofDescriptor = new ValueStateDescriptor<>(
            "featureBioCaptureTimeSpoofDescriptor", TypeInformation.of(new TypeHint<FeatureBioCaptureTimeSpoof.CaptureState>() {})
    );
    public static ValueStateDescriptor<FeatureBioLivenessDesync.DesyncState> featureBioLivenessDesyncDescriptor = new ValueStateDescriptor<>(
            "featureBioLivenessDesyncDescriptor", TypeInformation.of(new TypeHint<FeatureBioLivenessDesync.DesyncState>() {})
    );
    public static ValueStateDescriptor<FeatureBioExactImageSizeReplay.ImageSizeState> featureBioExactImageSizeReplayDescriptor = new ValueStateDescriptor<>(
            "featureBioExactImageSizeReplayDescriptor", TypeInformation.of(new TypeHint<FeatureBioExactImageSizeReplay.ImageSizeState>() {})
    );
    public static ValueStateDescriptor<FeatureBioUncannyValleyScorePattern.UncannyState> featureBioUncannyValleyScorePatternDescriptor = new ValueStateDescriptor<>(
            "featureBioUncannyValleyScorePatternDescriptor", TypeInformation.of(new TypeHint<FeatureBioUncannyValleyScorePattern.UncannyState>() {})
    );
    public static ValueStateDescriptor<FeatureBioDeepPrintSubversion.DeepPrintState> featureBioDeepPrintSubversionDescriptor = new ValueStateDescriptor<>(
            "featureBioDeepPrintSubversionDescriptor", TypeInformation.of(new TypeHint<FeatureBioDeepPrintSubversion.DeepPrintState>() {})
    );

    public static ValueStateDescriptor<FeatureBioFaceLivenessScoreDecline.FaceScoreState> featureBioFaceLivenessScoreDeclineDescriptor = new ValueStateDescriptor<>(
            "featureBioFaceLivenessScoreDeclineDescriptor", TypeInformation.of(new TypeHint<FeatureBioFaceLivenessScoreDecline.FaceScoreState>() {})
    );
    public static ValueStateDescriptor<FeatureBioModalityConcentration.ModalityState> featureBioModalityConcentrationDescriptor = new ValueStateDescriptor<>(
            "featureBioModalityConcentrationDescriptor", TypeInformation.of(new TypeHint<FeatureBioModalityConcentration.ModalityState>() {})
    );
    public static ValueStateDescriptor<FeatureBioImageSizeAnomaly.SizeState> featureBioImageSizeAnomalyDescriptor = new ValueStateDescriptor<>(
            "featureBioImageSizeAnomalyDescriptor", TypeInformation.of(new TypeHint<FeatureBioImageSizeAnomaly.SizeState>() {})
    );
    public static org.apache.flink.api.common.state.MapStateDescriptor<Long, java.util.List<com.foo.bar.dto.InputMessageTxn>> featureAuthBruteForceCompromiseBufferDescriptor = new org.apache.flink.api.common.state.MapStateDescriptor<>(
            "featureAuthBruteForceCompromiseBufferDescriptor",
            org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO,
            org.apache.flink.api.common.typeinfo.TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<java.util.List<com.foo.bar.dto.InputMessageTxn>>() {})
    );
    public static ValueStateDescriptor<FeatureAuthBruteForceCompromise.BruteForceState> featureAuthBruteForceCompromiseDescriptor = new ValueStateDescriptor<>(
            "featureAuthBruteForceCompromiseDescriptor", TypeInformation.of(new TypeHint<FeatureAuthBruteForceCompromise.BruteForceState>() {})
    );

    public static org.apache.flink.api.common.state.MapStateDescriptor<Long, java.util.List<com.foo.bar.dto.InputMessageTxn>> featureAuthMultiDevBufferDescriptor = new org.apache.flink.api.common.state.MapStateDescriptor<>(
            "featureAuthMultiDevBufferDescriptor",
            org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO,
            org.apache.flink.api.common.typeinfo.TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<java.util.List<com.foo.bar.dto.InputMessageTxn>>() {})
    );
    public static ValueStateDescriptor<FeatureAuthMultiDeviceConcurrency.ConcurrencyState> featureAuthMultiDeviceConcurrencyDescriptor = new ValueStateDescriptor<>(
            "featureAuthMultiDeviceConcurrencyDescriptor", TypeInformation.of(new TypeHint<FeatureAuthMultiDeviceConcurrency.ConcurrencyState>() {})
    );

    public static ValueStateDescriptor<FeatureAuthFailureStreak.FailureState> featureAuthFailureStreakDescriptor = new ValueStateDescriptor<>(
            "featureAuthFailureStreakDescriptor", TypeInformation.of(new TypeHint<FeatureAuthFailureStreak.FailureState>() {})
    );
}