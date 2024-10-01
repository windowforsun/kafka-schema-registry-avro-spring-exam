package com.windowforsun.kafka.avro.schemaregistry.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SendPaymentRequest {
    private String paymentId;
    private Double amount;
    private String currency;
    private String fromAccount;
    private String toAccount;
}
