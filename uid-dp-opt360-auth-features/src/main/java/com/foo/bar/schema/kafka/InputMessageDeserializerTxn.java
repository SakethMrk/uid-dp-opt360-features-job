package com.foo.bar.schema.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.foo.bar.dto.InputMessageTxn;
import java.util.Objects;

@Slf4j
public class InputMessageDeserializerTxn implements KafkaRecordDeserializationSchema<InputMessageTxn> {

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
	public TypeInformation<InputMessageTxn> getProducedType() {
		return TypeInformation.of(InputMessageTxn.class);
	}

	@Override
	public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<InputMessageTxn> out) {
		try{
			objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
			Decider topic = objectMapper.readValue(record.value(), Decider.class);

			if(Objects.equals(topic.getTopic(), "TXN"))
			{
				InputMessageTxn message = objectMapper.readValue(record.value(), InputMessageTxn.class);

				if(message!=null)
				{
					message.setKafkaSecondSourceTimestamp(record.timestamp());
					out.collect(message);
				}
				else
					log.info("Null message found in txn");
			}
		}
		catch (Exception ex){
			log.info("Problem message found in txn with exception: {}", (Object) ex.getStackTrace());
		}
	}
}