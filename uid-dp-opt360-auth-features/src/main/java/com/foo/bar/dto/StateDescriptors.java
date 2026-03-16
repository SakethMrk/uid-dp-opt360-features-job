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
}