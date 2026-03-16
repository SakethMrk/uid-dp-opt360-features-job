package com.foo.bar.pipeline;

import  com.foo.bar.dto.InputMessageBio;
import com.foo.bar.dto.OutMessage;
import com.foo.bar.functions.*;
import com.foo.bar.mapper.CommentsMapper;
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

        DataStream<OutMessage> unionStream = countsStreamHr1
                .union(gapStream1)
                .union(retryStream1)
                .union(livenessStreakStream);

        DataStream<OutMessage> enrichedStream = unionStream.map(new CommentsMapper());
        //DataStream<Row> rowStreamDevice = deviceChangeStream1.map(new RowMapperDevice());

        KafkaSink<OutMessage> kafkaSink = new Sinks().createKafkaSink(Driver.Configurations.KAFKA_OUTPUT_TOPIC, Driver.Configurations.KAFKA_BOOTSTRAP_SERVERS);
        enrichedStream.sinkTo(kafkaSink);

        DataStream<Row> rowStream = enrichedStream.map(new RowMapper());
        sink.prepareSink(rowStream);

        environment.execute("UID-KAFKA-AUTH TXN UNION-OPT360-AGG-FEATURE STREAM");
    }
}