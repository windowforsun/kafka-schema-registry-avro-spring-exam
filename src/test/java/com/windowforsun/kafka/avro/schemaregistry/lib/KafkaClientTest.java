package com.windowforsun.kafka.avro.schemaregistry.lib;

import com.windowforsun.avro.MyEvent;
import com.windowforsun.kafka.avro.schemaregistry.properties.DemoProperties;
import com.windowforsun.kafka.avro.schemaregistry.util.TestData;
import demo.kafka.event.PaymentSent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

@Slf4j
public class KafkaClientTest {
    private DemoProperties mockProperties;
    private KafkaTemplate mockKafkaTemplate;
    private KafkaClient kafkaClient;

    @BeforeEach
    public void setUp() {
        this.mockProperties = mock(DemoProperties.class);
        this.mockKafkaTemplate = mock(KafkaTemplate.class);
        this.kafkaClient = new KafkaClient(this.mockProperties, this.mockKafkaTemplate);
    }

    @Test
    public void testProcess_Success() throws Exception {
        String key = "test-key";
        String outboundEventId = UUID.randomUUID().toString();
        String topic = "test-outbound-topic";

        MyEvent myEvent = TestData.buildMyEvent(outboundEventId);
        ProducerRecord expectedRecord = new ProducerRecord<>(topic, key, myEvent);

        when(this.mockProperties.getOutboundTopic()).thenReturn(topic);
        ListenableFuture futureResult = mock(ListenableFuture.class);
        SendResult expectedSendResult = mock(SendResult.class);
        RecordMetadata metadata = new RecordMetadata(new TopicPartition(topic, 0), 0, 0, 0, 0L, 0, 0);
        when(futureResult.get()).thenReturn(expectedSendResult);
        when(expectedSendResult.getRecordMetadata()).thenReturn(metadata);
        when(this.mockKafkaTemplate.send(ArgumentMatchers.any(ProducerRecord.class))).thenReturn(futureResult);

        SendResult result = this.kafkaClient.sendMessage(key, myEvent);

        verify(this.mockKafkaTemplate, times(1)).send(expectedRecord);
        assertThat(result, is(expectedSendResult));
    }

    @Test
    public void testProcess_ExceptionOnSend() throws Exception {
        String key = "test-key";
        String outboundEventId = UUID.randomUUID().toString();
        String topic = "test-outbound-topic";

//        PaymentSent outboundEvent = TestData.buildPaymentSent(outboundEventId);
        MyEvent myEvent = TestData.buildMyEvent(outboundEventId);
        ProducerRecord expectedRecord = new ProducerRecord<>(topic, key, myEvent);

        when(this.mockProperties.getOutboundTopic()).thenReturn(topic);
        ListenableFuture futureResult = mock(ListenableFuture.class);
        SendResult expectedSendResult = mock(SendResult.class);
        RecordMetadata metadata = new RecordMetadata(new TopicPartition(topic, 0), 0, 0, 0, 0L, 0, 0);
        when(futureResult.get()).thenReturn(expectedSendResult);
        when(expectedSendResult.getRecordMetadata()).thenReturn(metadata);
        when(this.mockKafkaTemplate.send(ArgumentMatchers.any(ProducerRecord.class))).thenReturn(futureResult);

        doThrow(new ExecutionException("Kafka send failure", new Exception("Failed"))).when(futureResult).get();

        Exception exception = Assertions.assertThrows(RuntimeException.class, () -> {
            this.kafkaClient.sendMessage(key, myEvent);
        });

        verify(this.mockKafkaTemplate, times(1)).send(expectedRecord);
        assertThat(exception.getMessage(), is("Error sending message to topic " + topic));
    }
}
