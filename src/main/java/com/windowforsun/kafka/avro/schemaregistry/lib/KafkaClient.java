package com.windowforsun.kafka.avro.schemaregistry.lib;

import com.windowforsun.avro.MyEvent;
import com.windowforsun.kafka.avro.schemaregistry.properties.DemoProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaClient {
    private final DemoProperties properties;
    private final KafkaTemplate kafkaTemplate;

    public SendResult<String, MyEvent> sendMessage(String key, MyEvent event) {
        try {
            final ProducerRecord<String, MyEvent> record = new ProducerRecord<>(this.properties.getOutboundTopic(),
                    key,
                    event);
            final SendResult<String, MyEvent> result = (SendResult<String, MyEvent>) this.kafkaTemplate.send(record).get();
            final RecordMetadata metadata = result.getRecordMetadata();
            log.debug(String.format("Sent record(key=%s value=%s) meta(topic=%s, partition=%d, offset=%d)",
                    record.key(), record.value(), metadata.topic(), metadata.partition(), metadata.offset()));

            return result;
        } catch (Exception e) {
            String message = "Error sending message to topic " + this.properties.getOutboundTopic();
            log.error(message);
            throw new RuntimeException(message, e);
        }
    }
}
