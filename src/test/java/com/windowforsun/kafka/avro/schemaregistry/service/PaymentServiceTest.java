package com.windowforsun.kafka.avro.schemaregistry.service;

import com.windowforsun.kafka.avro.schemaregistry.lib.KafkaClient;
import com.windowforsun.kafka.avro.schemaregistry.request.SendPaymentRequest;
import com.windowforsun.kafka.avro.schemaregistry.util.TestData;
import demo.kafka.event.PaymentSent;
import demo.kafka.event.SendPayment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.mockito.Mockito.*;

public class PaymentServiceTest {
    private KafkaClient mockKafkaClient;
    private PaymentService service;

    @BeforeEach
    public void setUp() {
        this.mockKafkaClient = mock(KafkaClient.class);
        this.service = new PaymentService(this.mockKafkaClient);
    }

    @Test
    public void testProcess_ViaKafka() {
        String key = "test-key";
        SendPayment command = TestData.buildSendPayment(UUID.randomUUID().toString());
        this.service.process(key, command);

        verify(this.mockKafkaClient, times(1)).sendMessage(eq(key), any(PaymentSent.class));
    }

    @Test
    public void testProcess_Via_Rest() {
        String key = "test-key";
        SendPaymentRequest command = TestData.buildSendPaymentRequest(UUID.randomUUID().toString());
        this.service.process(key, command);

        verify(this.mockKafkaClient, times(1)).sendMessage(eq(key), any(PaymentSent.class));
    }
}
