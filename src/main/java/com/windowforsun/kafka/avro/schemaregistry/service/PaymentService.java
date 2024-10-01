package com.windowforsun.kafka.avro.schemaregistry.service;

import com.windowforsun.avro.MyEvent;
import com.windowforsun.kafka.avro.schemaregistry.lib.KafkaClient;
import com.windowforsun.kafka.avro.schemaregistry.request.SendEventRequest;
import com.windowforsun.kafka.avro.schemaregistry.request.SendPaymentRequest;
import demo.kafka.event.PaymentSent;
import demo.kafka.event.SendPayment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {
    private final KafkaClient kafkaClient;

    public void process(String key, SendPayment sendPayment) {
        this.sendPayment();

        PaymentSent outboundEvent = PaymentSent.newBuilder()
                .setPaymentId(sendPayment.getPaymentId())
                .setAmount(sendPayment.getAmount())
                .setCurrency(sendPayment.getCurrency())
                .setFromAccount(sendPayment.getFromAccount())
                .setToAccount(sendPayment.getToAccount())
                .build();

        this.kafkaClient.sendMessage(key, outboundEvent);
    }

    public void process(String key, SendPaymentRequest request) {
        this.sendPayment();
        PaymentSent outboundEvent = PaymentSent.newBuilder()
                .setPaymentId(request.getPaymentId())
                .setAmount(request.getAmount())
                .setCurrency(request.getCurrency())
                .setFromAccount(request.getFromAccount())
                .setToAccount(request.getToAccount())
                .build();

        this.kafkaClient.sendMessage(key, outboundEvent);
    }



    private void sendPayment() {
        // no implementation, just simulating
    }
}
