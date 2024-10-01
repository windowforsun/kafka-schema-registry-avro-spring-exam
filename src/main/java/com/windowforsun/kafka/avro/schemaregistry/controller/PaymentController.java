package com.windowforsun.kafka.avro.schemaregistry.controller;

import com.windowforsun.kafka.avro.schemaregistry.request.SendPaymentRequest;
import com.windowforsun.kafka.avro.schemaregistry.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/v1/payments")
@RequiredArgsConstructor
public class PaymentController {
    private final PaymentService paymentService;

    @PostMapping("/send")
    public ResponseEntity<String> sendPayment(@RequestBody SendPaymentRequest request) {
        try {
            this.paymentService.process(request.getPaymentId(), request);

            return ResponseEntity.ok(request.getPaymentId());
        } catch (Exception e) {
            log.error(e.getMessage());
            return ResponseEntity.internalServerError().body(request.getPaymentId());
        }
    }
}
