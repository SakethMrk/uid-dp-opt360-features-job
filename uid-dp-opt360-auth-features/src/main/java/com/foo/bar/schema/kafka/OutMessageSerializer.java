package com.foo.bar.schema.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.bar.dto.OutMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Slf4j
public class OutMessageSerializer implements KafkaRecordSerializationSchema<OutMessage> {
    private final String topic;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OutMessageSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(OutMessage element, KafkaSinkContext context, Long timestamp) {
        try {
            byte[] value = objectMapper.writeValueAsBytes(element);
            return new ProducerRecord<>(topic, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8), value);
        }
        catch(JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}