package com.windowforsun.kafka.avro.schemaregistry.controller;

import com.windowforsun.kafka.avro.schemaregistry.request.SendEventRequest;
import com.windowforsun.kafka.avro.schemaregistry.service.MyEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/v1/event")
@RequiredArgsConstructor
public class MyEventController {
    private final MyEventService myEventService;

    @PostMapping("/send")
    public ResponseEntity<String> sendEvent(@RequestBody SendEventRequest request) {
        try {
            this.myEventService.process(request.getEventId(), request);

            return ResponseEntity.ok(request.getEventId());
        } catch (Exception e) {
            log.error(e.getMessage());
            return ResponseEntity.internalServerError().body(request.getEventId());
        }
    }
}
