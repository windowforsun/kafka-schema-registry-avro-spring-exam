package com.windowforsun.kafka.avro.schemaregistry.util;

import com.windowforsun.avro.MyEvent;
import com.windowforsun.avro.SendEvent;
import com.windowforsun.kafka.avro.schemaregistry.request.SendEventRequest;
import com.windowforsun.kafka.avro.schemaregistry.request.SendPaymentRequest;
import demo.kafka.event.PaymentSent;
import demo.kafka.event.SendPayment;

public class TestData {
    public static MyEvent buildMyEvent(String eventId) {
        return MyEvent.newBuilder()
                .setEventId(eventId)
                .setNumber(1111L)
                .build();
    }

    public static SendEvent buildSendEvent(String eventId) {
        return SendEvent.newBuilder()
                .setEventId(eventId)
                .setNumber(1111L)
                .build();
    }

    public static SendEventRequest buildSendEventRequest(String eventId) {
        return SendEventRequest.builder()
                .eventId(eventId)
                .number(1111L)
                .build();
    }

    public static PaymentSent buildPaymentSent(String paymentId) {
        return PaymentSent.newBuilder()
                .setPaymentId(paymentId)
                .setAmount(2.0)
                .setCurrency("USD")
                .setToAccount("toAcc")
                .setFromAccount("fromAcc")
                .build();
    }

    public static SendPayment buildSendPayment(String paymentId) {
        return SendPayment.newBuilder()
                .setPaymentId(paymentId)
                .setAmount(2.0)
                .setCurrency("USD")
                .setToAccount("toAcc")
                .setFromAccount("fromAcc")
                .build();
    }

    public static SendPaymentRequest buildSendPaymentRequest(String paymentId) {
        return SendPaymentRequest.builder()
                .paymentId(paymentId)
                .amount(10.00)
                .currency("USD")
                .fromAccount("from-acct")
                .toAccount("to-acct")
                .build();
    }
}
