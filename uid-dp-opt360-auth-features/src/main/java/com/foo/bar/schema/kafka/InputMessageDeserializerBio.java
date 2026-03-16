package com.foo.bar.schema.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.InputMessageBio;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.Objects;

@Slf4j
public class InputMessageDeserializerBio implements KafkaRecordDeserializationSchema<InputMessageBio> {

	@Getter
	@Setter
	@NoArgsConstructor
	@ToString
	private static class Decider{

		@JsonProperty("topic")
		private String topic;
	}

	private final ObjectMapper objectMapper = new ObjectMapper();
	@Override
	public TypeInformation<InputMessageBio> getProducedType() {
		return TypeInformation.of(InputMessageBio.class);
	}

	@Override
	public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<InputMessageBio> out) {
		try{
			objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
			Decider topic = objectMapper.readValue(record.value(), Decider.class);

			if(Objects.equals(topic.getTopic(), "BIO"))
			{
				InputMessageBio message = objectMapper.readValue(record.value(), InputMessageBio.class);

				if(message!=null)
				{
					message.setKafkaSecondSourceTimestamp(record.timestamp());
					out.collect(message);
				}
				else
					log.info("Null message found in bio");
			}
		}
		catch (Exception ex){
			log.info("Problem message found in bio with exception: {}", (Object) ex.getStackTrace());
		}
	}
}