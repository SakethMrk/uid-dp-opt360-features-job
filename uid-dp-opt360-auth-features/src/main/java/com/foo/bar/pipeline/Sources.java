package com.foo.bar.pipeline;

import java.util.Collections;
import com.foo.bar.dto.InputMessageBio;
import com.foo.bar.schema.kafka.InputMessageDeserializerBio;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import com.foo.bar.dto.InputMessageTxn;
import com.foo.bar.schema.kafka.InputMessageDeserializerTxn;

public class Sources {

	public KafkaSource<InputMessageTxn> kafkaSourceConsumerTxn(String inTopic, String kafkaBroker, String consumerGroup) {
		return KafkaSource.<InputMessageTxn>builder()
				.setBootstrapServers(kafkaBroker)
				.setGroupId(consumerGroup)
				.setTopics(Collections.singletonList(inTopic))
				.setDeserializer(new InputMessageDeserializerTxn())
				.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
				.build();
	}

	public KafkaSource<InputMessageBio> kafkaSourceConsumerBio(String inTopic, String kafkaBroker, String consumerGroup) {
		return KafkaSource.<InputMessageBio>builder()
				.setBootstrapServers(kafkaBroker)
				.setGroupId(consumerGroup)
				.setTopics(Collections.singletonList(inTopic))
				.setDeserializer(new InputMessageDeserializerBio())
				.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
				.build();
	}
}