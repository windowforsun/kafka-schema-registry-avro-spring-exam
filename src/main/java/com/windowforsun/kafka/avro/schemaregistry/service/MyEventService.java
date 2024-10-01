package com.windowforsun.kafka.avro.schemaregistry.service;

import com.windowforsun.avro.MyEvent;
import com.windowforsun.avro.SendEvent;
import com.windowforsun.kafka.avro.schemaregistry.lib.KafkaClient;
import com.windowforsun.kafka.avro.schemaregistry.request.SendEventRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class MyEventService {
    private final KafkaClient kafkaClient;

    public void process(String key, SendEvent event) {
        this.sendPayment();

        MyEvent myEvent = MyEvent.newBuilder()
                .setEventId(event.getEventId())
                .setNumber(event.getNumber())
                .build();

        this.kafkaClient.sendMessage(key, myEvent);
    }

    public void process(String key, SendEventRequest event) {
        this.sendPayment();

        MyEvent myEvent = MyEvent.newBuilder()
                .setEventId(event.getEventId())
                .setNumber(event.getNumber())
                .build();

        this.kafkaClient.sendMessage(key, myEvent);
    }



    private void sendPayment() {
        // no implementation, just simulating
    }
}
