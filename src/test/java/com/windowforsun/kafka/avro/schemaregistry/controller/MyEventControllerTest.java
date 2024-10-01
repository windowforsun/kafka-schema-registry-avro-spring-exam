package com.windowforsun.kafka.avro.schemaregistry.controller;

import com.windowforsun.kafka.avro.schemaregistry.request.SendEventRequest;
import com.windowforsun.kafka.avro.schemaregistry.service.MyEventService;
import com.windowforsun.kafka.avro.schemaregistry.util.TestData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.UUID;

import static org.mockito.Mockito.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class MyEventControllerTest {
    private MyEventService serviceMock;
    private MyEventController controller;

    @BeforeEach
    public void setUp() {
        this.serviceMock = mock(MyEventService.class);
        this.controller = new MyEventController(this.serviceMock);
    }

    @Test
    public void testListen_Success() {
        SendEventRequest request = TestData.buildSendEventRequest(UUID.randomUUID().toString());
        ResponseEntity response = this.controller.sendEvent(request);

        assertThat(response.getStatusCode(), is(HttpStatus.OK));
        assertThat(response.getBody(), is(request.getEventId()));
        verify(this.serviceMock, times(1))
                .process(request.getEventId(), request);
    }

    @Test
    public void testListen_ServiceThrowsException() {
        SendEventRequest request = TestData.buildSendEventRequest(UUID.randomUUID().toString());

        doThrow(new RuntimeException("fail"))
                .when(this.serviceMock)
                .process(request.getEventId(), request);

        ResponseEntity response = this.controller.sendEvent(request);

        assertThat(response.getStatusCode(), is(HttpStatus.INTERNAL_SERVER_ERROR));
        assertThat(response.getBody(), is(request.getEventId()));
        verify(this.serviceMock, times(1))
                .process(request.getEventId(), request);
    }
}
