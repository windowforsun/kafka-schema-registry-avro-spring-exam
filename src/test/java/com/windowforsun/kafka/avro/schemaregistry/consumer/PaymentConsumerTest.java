package com.windowforsun.kafka.avro.schemaregistry.consumer;

import com.windowforsun.kafka.avro.schemaregistry.service.PaymentService;
import com.windowforsun.kafka.avro.schemaregistry.util.TestData;
import demo.kafka.event.SendPayment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.mockito.Mockito.*;

public class PaymentConsumerTest {
    private PaymentService serviceMock;
    private PaymentConsumer consumer;

    @BeforeEach
    public void setUp() {
        this.serviceMock = mock(PaymentService.class);
        this.consumer = new PaymentConsumer(this.serviceMock);
    }

    @Test
    public void testListen_Success() {
        String key = "test-key";
        SendPayment command = TestData.buildSendPayment(UUID.randomUUID().toString());
        this.consumer.listen(key, command);

        verify(this.serviceMock, times(1)).process(key, command);
    }

    @Test
    public void testListen_ServiceThrowsException() {
        String key = "test-key";
        SendPayment command = TestData.buildSendPayment(UUID.randomUUID().toString());

        doThrow(new RuntimeException("Service failure")).when(this.serviceMock).process(key, command);

        this.consumer.listen(key, command);

        verify(this.serviceMock, times(1)).process(key, command);
    }
}
