package com.windowforsun.kafka.avro.schemaregistry.integration;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.windowforsun.avro.MyEvent;
import com.windowforsun.avro.SendEvent;
import com.windowforsun.kafka.avro.schemaregistry.SpringDemoConfig;
import com.windowforsun.kafka.avro.schemaregistry.request.SendEventRequest;
import com.windowforsun.kafka.avro.schemaregistry.util.TestData;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@Slf4j
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = {SpringDemoConfig.class, TestSpringDemoConfig.class})
@DirtiesContext(classMode =  DirtiesContext.ClassMode.AFTER_CLASS)
@AutoConfigureWireMock(port = 0)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true, topics = {"my-event"})
public class MyEventIntegrationTest {
    private final static String SEND_EVENT_TOPIC = "send-event";
    private final static String MY_EVENT_TOPIC = "my-event";

    @Autowired
    private TestRestTemplate restTemplate;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private KafkaListenerEndpointRegistry registry;
    @Autowired
    private TestSpringDemoConfig.KafkaTestListener testReceiver;
    @Autowired
    private KafkaTemplate testKafkaTemplate;


//    @Configuration
//    static class TestConfig {
//        @Bean
//        public KafkaTestListener testReceiver() {
//            return new KafkaTestListener();
//        }
//    }

//    public static class KafkaTestListener {
//        AtomicInteger counter = new AtomicInteger(0);
//
//        @KafkaListener(groupId = "MyEventIntegrationTest", topics = "my-event", containerFactory = "testKafkaListenerContainerFactory", autoStartup = "true")
//        void receive(@Payload final MyEvent payload) {
//            log.debug("KafkaTestListener - Received with Id: " + payload.getEventId());
//            counter.incrementAndGet();
//        }
//    }

    @BeforeEach
    public void setUp() throws Exception {
        this.registry.getListenerContainers()
                .stream()
                .forEach(container ->
                        ContainerTestUtils.waitForAssignment(container, this.embeddedKafkaBroker.getPartitionsPerTopic()));
        this.testReceiver.counter.set(0);

        WireMock.reset();
        WireMock.resetAllRequests();
        WireMock.resetAllScenarios();
        WireMock.resetToDefault();

        this.registerSchema(1, SEND_EVENT_TOPIC, SendEvent.getClassSchema().toString());
        this.registerSchema(2, MY_EVENT_TOPIC, MyEvent.getClassSchema().toString());
    }

    @Test
    public void testSendEventViaRest() {
        int totalMessages = 10;

        for(int i = 0; i < totalMessages; i++) {
            String eventId = UUID.randomUUID().toString();

            SendEventRequest request = TestData.buildSendEventRequest(eventId);
            this.sendRequestViaRest(request);
        }

        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiver.counter::get, is(totalMessages));
    }

    @Test
    public void testSendEventViaKafka() throws Exception {
        int totalMessages = 10;

        for(int i = 0; i < totalMessages; i++) {
            String eventId = UUID.randomUUID().toString();

            SendEvent event = TestData.buildSendEvent(eventId);
            this.sendCommandViaKafka(event);
        }

        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiver.counter::get, is(totalMessages));
    }

    private void registerSchema(int schemaId, String topic, String schema) throws Exception {
        stubFor(post(urlPathMatching("/subjects/" + topic + "-value"))
                .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody("{\"id\":" + schemaId + "}")));

        final SchemaString schemaString = new SchemaString(schema);
        stubFor(get(urlPathMatching("/schemas/ids/" + schemaId))
                .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(schemaString.toJson())));
    }


    private void sendRequestViaRest(SendEventRequest request) {
        ResponseEntity<String> response = restTemplate.postForEntity("/v1/event/send", request, String.class);
        assertThat(response.getStatusCode(), equalTo(HttpStatus.OK));
        assertThat(response.getBody(), is(request.getEventId()));
    }

    private void sendCommandViaKafka(SendEvent event) throws Exception {
        final ProducerRecord<Long, String> record = new ProducerRecord(MY_EVENT_TOPIC, null, event.getEventId(), event);
        this.testKafkaTemplate.send(record).get();
    }
}
