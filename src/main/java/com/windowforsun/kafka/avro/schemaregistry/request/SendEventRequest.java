package com.windowforsun.kafka.avro.schemaregistry.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SendEventRequest {
    private String eventId;
    private Long number;
}
