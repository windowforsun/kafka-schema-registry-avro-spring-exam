package com.windowforsun.kafka.avro.schemaregistry.service;

import com.windowforsun.avro.MyEvent;
import com.windowforsun.avro.SendEvent;
import com.windowforsun.kafka.avro.schemaregistry.lib.KafkaClient;
import com.windowforsun.kafka.avro.schemaregistry.request.SendEventRequest;
import com.windowforsun.kafka.avro.schemaregistry.util.TestData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.mockito.Mockito.*;

public class MyEventServiceTest {
    private KafkaClient kafkaClientMock;
    private MyEventService myEventService;

    @BeforeEach
    public void setUp() {
        this.kafkaClientMock = mock(KafkaClient.class);
        this.myEventService = new MyEventService(this.kafkaClientMock);
    }

    @Test
    public void testProcess_ViaKafka() {
        String key = "test-key";
        SendEvent sendEvent = TestData.buildSendEvent(UUID.randomUUID().toString());
        this.myEventService.process(key, sendEvent);

        verify(this.kafkaClientMock, times(1))
                .sendMessage(eq(key), any(MyEvent.class));
    }

    @Test
    public void testProcess_Vai_Rest() {
        String key = "test-key";
        SendEventRequest sendEventRequest = TestData.buildSendEventRequest(UUID.randomUUID().toString());
        this.myEventService.process(key, sendEventRequest);

        verify(this.kafkaClientMock, times(1))
                .sendMessage(eq(key), any(MyEvent.class));
    }
}
