package com.windowforsun.kafka.avro.schemaregistry.consumer;

import com.windowforsun.avro.SendEvent;
import com.windowforsun.kafka.avro.schemaregistry.service.MyEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class MyEventConsumer {
    final AtomicInteger counter = new AtomicInteger();
    private final MyEventService myEventService;

    @KafkaListener(topics = "send-event", groupId = "demo-consumer-group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key, @Payload final SendEvent sendEvent) {
        this.counter.getAndIncrement();
        log.debug("Received message [{}] - key : {}", this.counter.get(), key);

        try {
            this.myEventService.process(key, sendEvent);
        } catch (Exception e) {
            log.error("Error processing message : {}", e.getMessage());
        }

    }
}
