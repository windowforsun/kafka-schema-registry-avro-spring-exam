package com.windowforsun.kafka.avro.schemaregistry.controller;

import com.windowforsun.kafka.avro.schemaregistry.request.SendPaymentRequest;
import com.windowforsun.kafka.avro.schemaregistry.service.PaymentService;
import com.windowforsun.kafka.avro.schemaregistry.util.TestData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.UUID;

import static org.mockito.Mockito.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

public class PaymentControllerTest {
    private PaymentService serviceMock;
    private PaymentController controller;

    @BeforeEach
    public void setUp() {
        this.serviceMock = mock(PaymentService.class);
        this.controller = new PaymentController(this.serviceMock);
    }

    @Test
    public void testListen_Success() {
        SendPaymentRequest request = TestData.buildSendPaymentRequest(UUID.randomUUID().toString());
        ResponseEntity response = this.controller.sendPayment(request);

        assertThat(response.getStatusCode(), is(HttpStatus.OK));
        assertThat(response.getBody(), is(request.getPaymentId()));
        verify(this.serviceMock, times(1)).process(request.getPaymentId(), request);
    }

    @Test
    public void testListen_ServiceThrowsException() {
        SendPaymentRequest request = TestData.buildSendPaymentRequest(UUID.randomUUID().toString());

        doThrow(new RuntimeException("Service failure")).when(this.serviceMock).process(request.getPaymentId(), request);

        ResponseEntity response = this.controller.sendPayment(request);

        assertThat(response.getStatusCode(), is(HttpStatus.INTERNAL_SERVER_ERROR));
        assertThat(response.getBody(), is(request.getPaymentId()));
        verify(this.serviceMock, times(1)).process(request.getPaymentId(), request);
    }
}
