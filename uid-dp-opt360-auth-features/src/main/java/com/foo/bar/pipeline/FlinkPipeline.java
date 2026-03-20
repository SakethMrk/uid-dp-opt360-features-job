package com.foo.bar.pipeline;

import  com.foo.bar.dto.InputMessageBio;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.functions.*;
import com.foo.bar.functions.bio.*;
import com.foo.bar.mapper.FunctionMapTxn;
import com.foo.bar.mapper.RowMapper;
import com.foo.bar.mapper.RowMapperDevice;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.foo.bar.dto.InputMessageTxn;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

@Slf4j
public class FlinkPipeline {

    public void compose() throws Exception {

        Sources source = new Sources();
        Sinks sink = new Sinks();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .enableCheckpointing(Driver.Configurations.CHECK_POINT_INTERVAL, CheckpointingMode.EXACTLY_ONCE)
                .setParallelism(4);

        Configuration cfg = new Configuration();
        cfg.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        cfg.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, Driver.Configurations.CHECK_POINT_STORAGE);
        environment.configure(cfg);

        environment.getCheckpointConfig().setCheckpointTimeout(30 * 60 * 1000);

        EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend(true);
        stateBackend.setRocksDBOptions(new Options());
        environment.setStateBackend(stateBackend);

        KafkaSource<InputMessageTxn> kafkaSource1 = source.kafkaSourceConsumerTxn(Driver.Configurations.KAFKA_TOPIC,
                Driver.Configurations.KAFKA_BOOTSTRAP_SERVERS, "STROT.APPLICATION.OPT360_AUTH_TEST");

        KafkaSource<InputMessageBio> kafkaSource2 = source.kafkaSourceConsumerBio(Driver.Configurations.KAFKA_TOPIC,
                Driver.Configurations.KAFKA_BOOTSTRAP_SERVERS, "STROT.APPLICATION.OPT360_AUTH_test");

        DataStream<InputMessageTxn> inStream1 = environment.fromSource(kafkaSource1,
                WatermarkStrategy.forMonotonousTimestamps(), "hdc-auth-txn-union-source");

        DataStream<InputMessageBio> inStream2 = environment.fromSource(kafkaSource2,
                WatermarkStrategy.forMonotonousTimestamps(), "hdc-auth-bio-union-source");

        DataStream<OutMessage> countsStreamHr1 = inStream1
                .assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy())
                .keyBy(InputMessageTxn::getOptId)
                .window(TumblingEventTimeWindows.of(Time.minutes(60), Time.minutes(30)))
                .aggregate(new FeatureAuthCounts.CountAggregator(),
                        new FeatureAuthCounts.WindowResultFunction());

//        DataStream<OutMessage> countsStreamMin1 = inStream1
//                .assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy())
//                .keyBy(InputMessageTxn::getOptId)
//                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
//                .aggregate(new FeatureAuthCounts.CountAggregator(),
//                        new FeatureAuthCounts.WindowResultFunction());

//        DataStream<OutMessage> gapAvgStream1 = inStream1
//                .assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy())
//                .keyBy(InputMessageTxn::getOptId)
//                .process(new FeatureAuthGapAvg());

        DataStream<OutMessage> gapStream1 = inStream1
                .assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy())
                .keyBy(InputMessageTxn::getOptId)
                .process(new FeatureAuthGap());

        DataStream<OutMessage> retryStream1 = inStream1
                .assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy())
                .keyBy(InputMessageTxn::getOptId)
                .process(new FeatureAuthRetrySession());

//        DataStream<OutMessage.OutMessageDeviceChange> deviceChangeStream1 = inStream1
//                .filter(msg -> msg.getDeviceCode()!=null && !msg.getDeviceCode().isEmpty())
//                .assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy())
//                .keyBy(InputMessageTxn::getOptId)
//                .process(new FeatureAuthDeviceChange());

//        DataStream<OutMessage> oddHourStream1 = inStream1
//                .assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy())
//                .keyBy(InputMessageTxn::getOptId)
//                .process(new FeatureAuthOddHourRatio());

        DataStream<OutMessage> livenessStreakStream = inStream1.
                assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy())
                .keyBy(InputMessageTxn::getOptId)
                .process(new FeatureLivenessStreakScore());

        // --- NEW TXN FEATURES STREAMS ---
        DataStream<OutMessage> fAuthFailureRate = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthFailureRate());
        DataStream<OutMessage> fAuthOtpFallback = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthOtpFallbackRatio());
        DataStream<OutMessage> fAuthSuccessNoBio = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthSuccessWithoutBio());
        DataStream<OutMessage> fAuthVelocitySpike = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthVelocitySpike());
        DataStream<OutMessage> fAuthFailureBurst = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthFailureBurst());
        DataStream<OutMessage> fAuthGeoDrift = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthGeoDrift());
        DataStream<OutMessage> fAuthLocResMismatch = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthLocationResidentMismatch());
        DataStream<OutMessage> fAuthDevDiversity = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthDeviceDiversity());
        DataStream<OutMessage> fAuthModDiversity = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthModelDiversity());
        DataStream<OutMessage> fAuthRdSoftChange = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthRdSoftwareChange());
        DataStream<OutMessage> fAuthDevProvSwitch = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthDeviceProviderSwitch());
        DataStream<OutMessage> fAuthFinScoreDec = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthFingerScoreDecline());
        DataStream<OutMessage> fAuthFaceScoreDec = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthFaceScoreDecline());
        DataStream<OutMessage> fAuthFmrCountAnom = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthFmrCountAnomaly());
        DataStream<OutMessage> fAuthPidSizeAnom = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthPidSizeAnomaly());
        DataStream<OutMessage> fAuthTimeEntropy = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthTimeEntropy());
        DataStream<OutMessage> fAuthWeekendAnom = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthWeekendAnomaly());
        DataStream<OutMessage> fAuthAuaSaDiv = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthAuaSaDiversity());
        DataStream<OutMessage> fAuthEnrolHammer = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthEnrolmentHammering());
        DataStream<OutMessage> fAuthDurOutlier = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthDurationOutlier());
        DataStream<OutMessage> fAuthSrvConcent = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthServerConcentration());
        DataStream<OutMessage> fAuthCertExpiry = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthCertExpiry());
        DataStream<OutMessage> fAuthErrHotspot = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthErrorCodeHotspot());
        DataStream<OutMessage> fAuthModSwitch = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthModalitySwitch());

        DataStream<OutMessage> fAuthGhostUp = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthGhostUptime());
        DataStream<OutMessage> fAuthCrossVel = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthCrossBorderVelocity());
        DataStream<OutMessage> fAuthConcAua = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthConcurrentAuaBurst());
        DataStream<OutMessage> fAuthBioRep = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthBiometricReplay());
        DataStream<OutMessage> fAuthHiRisk = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthHighRiskTimeWindow());
        DataStream<OutMessage> fAuthFailRec = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthFailureRecoverySpeed());
        DataStream<OutMessage> fAuthRapDevSw = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthRapidDeviceSwitch());
        DataStream<OutMessage> fAuthErrSpray = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthMultiErrorSpray());
        DataStream<OutMessage> fAuthNightSrg = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthNightSurge());
        DataStream<OutMessage> fAuthConsIdent = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthConsecutiveIdenticalEvents());

        // --- NEW FAULT-TOLERANT FEATURES ---
        DataStream<OutMessage> fAuthFailStreak = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthFailureStreak());
        DataStream<OutMessage> fAuthBruteForce = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthBruteForceCompromise());
        DataStream<OutMessage> fAuthConcurrency = inStream1.assignTimestampsAndWatermarks(WatermarkStrategies.getWatermarkStrategy()).keyBy(InputMessageTxn::getOptId).process(new FeatureAuthMultiDeviceConcurrency());

        // --- NEW BIO FEATURES STREAMS ---
        DataStream<OutMessage> fBioMatchTrend = inStream2.assignTimestampsAndWatermarks(WatermarkStrategiesBio.getWatermarkStrategy()).keyBy(InputMessageBio::getOptId).process(new FeatureBioMatchScoreTrend());
        DataStream<OutMessage> fBioLivFailStrk = inStream2.assignTimestampsAndWatermarks(WatermarkStrategiesBio.getWatermarkStrategy()).keyBy(InputMessageBio::getOptId).process(new FeatureBioLivenessFailureStreak());
        DataStream<OutMessage> fBioRespTimeAnom = inStream2.assignTimestampsAndWatermarks(WatermarkStrategiesBio.getWatermarkStrategy()).keyBy(InputMessageBio::getOptId).process(new FeatureBioResponseTimeAnomaly());
        DataStream<OutMessage> fBioDeepPrntAnom = inStream2.assignTimestampsAndWatermarks(WatermarkStrategiesBio.getWatermarkStrategy()).keyBy(InputMessageBio::getOptId).process(new FeatureBioDeepPrintAnomaly());
        DataStream<OutMessage> fBioFaceDecline = inStream2.assignTimestampsAndWatermarks(WatermarkStrategiesBio.getWatermarkStrategy()).keyBy(InputMessageBio::getOptId).process(new FeatureBioFaceLivenessScoreDecline());
        DataStream<OutMessage> fBioDeepAnom = inStream2.assignTimestampsAndWatermarks(WatermarkStrategiesBio.getWatermarkStrategy()).keyBy(InputMessageBio::getOptId).process(new FeatureBioDeepPrintAnomaly());
        DataStream<OutMessage> fBioModConc = inStream2.assignTimestampsAndWatermarks(WatermarkStrategiesBio.getWatermarkStrategy()).keyBy(InputMessageBio::getOptId).process(new FeatureBioModalityConcentration());

        DataStream<OutMessage> fBioCapTime = inStream2.assignTimestampsAndWatermarks(WatermarkStrategiesBio.getWatermarkStrategy()).keyBy(InputMessageBio::getOptId).process(new FeatureBioCaptureTimeSpoof());
        DataStream<OutMessage> fBioLivDesync = inStream2.assignTimestampsAndWatermarks(WatermarkStrategiesBio.getWatermarkStrategy()).keyBy(InputMessageBio::getOptId).process(new FeatureBioLivenessDesync());
        DataStream<OutMessage> fBioImgSize = inStream2.assignTimestampsAndWatermarks(WatermarkStrategiesBio.getWatermarkStrategy()).keyBy(InputMessageBio::getOptId).process(new FeatureBioExactImageSizeReplay());
        DataStream<OutMessage> fBioUncanny = inStream2.assignTimestampsAndWatermarks(WatermarkStrategiesBio.getWatermarkStrategy()).keyBy(InputMessageBio::getOptId).process(new FeatureBioUncannyValleyScorePattern());
        DataStream<OutMessage> fBioDeepSub = inStream2.assignTimestampsAndWatermarks(WatermarkStrategiesBio.getWatermarkStrategy()).keyBy(InputMessageBio::getOptId).process(new FeatureBioDeepPrintSubversion());

        // Union feature streams
        DataStream<OutMessage> unionStream = countsStreamHr1
                .union(gapStream1)
                .union(retryStream1)
                .union(livenessStreakStream)
                .union(fAuthFailureRate)
                .union(fAuthOtpFallback)
                .union(fAuthSuccessNoBio)
                .union(fAuthVelocitySpike)
                .union(fAuthFailureBurst)
                .union(fAuthGeoDrift)
                .union(fAuthLocResMismatch)
                .union(fAuthDevDiversity)
                .union(fAuthModDiversity)
                .union(fAuthRdSoftChange)
                .union(fAuthDevProvSwitch)
                .union(fAuthFinScoreDec)
                .union(fAuthFaceScoreDec)
                .union(fAuthFmrCountAnom)
                .union(fAuthPidSizeAnom)
                .union(fAuthTimeEntropy)
                .union(fAuthWeekendAnom)
                .union(fAuthAuaSaDiv)
                .union(fAuthEnrolHammer)
                .union(fAuthDurOutlier)
                .union(fAuthSrvConcent)
                .union(fAuthCertExpiry)
                .union(fAuthErrHotspot)
                .union(fAuthModSwitch)
                .union(fAuthGhostUp)
                .union(fAuthCrossVel)
                .union(fAuthConcAua)
                .union(fAuthBioRep)
                .union(fAuthHiRisk)
                .union(fAuthFailRec)
                .union(fAuthRapDevSw)
                .union(fAuthErrSpray)
                .union(fAuthNightSrg)
                .union(fAuthConsIdent)
                .union(fAuthFailStreak)
                .union(fAuthBruteForce)
                .union(fAuthConcurrency)
                .union(fBioMatchTrend)
                .union(fBioLivFailStrk)
                .union(fBioRespTimeAnom)
                .union(fBioDeepPrntAnom)
                .union(fBioFaceDecline)
                .union(fBioDeepAnom)
                .union(fBioModConc)
                .union(fBioCapTime)
                .union(fBioLivDesync)
                .union(fBioImgSize)
                .union(fBioUncanny)
                .union(fBioDeepSub);

        DataStream<OutMessage> enrichedStream = unionStream.map(new CommentsMapper());
        //DataStream<Row> rowStreamDevice = deviceChangeStream1.map(new RowMapperDevice());

        KafkaSink<OutMessage> kafkaSink = new Sinks().createKafkaSink(Driver.Configurations.KAFKA_OUTPUT_TOPIC, Driver.Configurations.KAFKA_BOOTSTRAP_SERVERS);
        enrichedStream.sinkTo(kafkaSink);

        DataStream<Row> rowStream = enrichedStream.map(new RowMapper());
        sink.prepareSink(rowStream);

        environment.execute("UID-KAFKA-AUTH TXN UNION-OPT360-AGG-FEATURE STREAM");
    }
}