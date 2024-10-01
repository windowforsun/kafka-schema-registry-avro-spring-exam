package com.windowforsun.kafka.avro.schemaregistry.util;

import com.windowforsun.avro.MyEvent;
import com.windowforsun.avro.SendEvent;
import com.windowforsun.kafka.avro.schemaregistry.request.SendEventRequest;

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
}
