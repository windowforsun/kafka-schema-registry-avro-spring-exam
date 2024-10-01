package com.windowforsun.kafka.avro.schemaregistry.consumer;

import com.windowforsun.avro.SendEvent;
import com.windowforsun.kafka.avro.schemaregistry.service.MyEventService;
import com.windowforsun.kafka.avro.schemaregistry.util.TestData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.mockito.Mockito.*;

public class MyEventConsumerTest {
    private MyEventService serviceMock;
    private MyEventConsumer consumer;

    @BeforeEach
    public void setUp() {
        this.serviceMock = mock(MyEventService.class);
        this.consumer = new MyEventConsumer(this.serviceMock);
    }

    @Test
    public void testListen_Success() {
        String key = "test-key";
        SendEvent sendEvent = TestData.buildSendEvent(UUID.randomUUID().toString());
        this.consumer.listen(key, sendEvent);

        verify(this.serviceMock, times(1)).process(key, sendEvent);
    }

    @Test
    public void testListen_ServiceThrowsException() {
        String key = "test-key";
        SendEvent sendEvent = TestData.buildSendEvent(UUID.randomUUID().toString());

        doThrow(new RuntimeException("fail"))
                .when(this.serviceMock)
                .process(key, sendEvent);

        this.consumer.listen(key, sendEvent);

        verify(this.serviceMock, times(1)).process(key, sendEvent);
    }
}
