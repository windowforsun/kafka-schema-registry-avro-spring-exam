package com.windowforsun.kafka.avro.schemaregistry.consumer;

import com.windowforsun.kafka.avro.schemaregistry.service.PaymentService;
import demo.kafka.event.SendPayment;
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
public class PaymentConsumer {
    final AtomicInteger counter = new AtomicInteger();
    private final PaymentService paymentService;

    @KafkaListener(topics = "send-payment", groupId = "demo-consumer-group",containerFactory = "kafkaListenerContainerFactory")
    public void listen(@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                       @Payload final SendPayment sendPayment) {
        this.counter.getAndIncrement();
        log.debug("Received message [{}] - key : {}", this.counter.get(), key);

        try {
            this.paymentService.process(key, sendPayment);
        } catch (Exception e) {
            log.error("Error processing message : {}", e.getMessage());
        }
    }
}
