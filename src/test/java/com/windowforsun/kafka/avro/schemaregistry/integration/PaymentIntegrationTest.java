package com.windowforsun.kafka.avro.schemaregistry.integration;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.windowforsun.kafka.avro.schemaregistry.SpringDemoConfig;
import com.windowforsun.kafka.avro.schemaregistry.request.SendPaymentRequest;
import com.windowforsun.kafka.avro.schemaregistry.util.TestData;
import demo.kafka.event.PaymentSent;
import demo.kafka.event.SendPayment;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.*;

@Slf4j
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = {SpringDemoConfig.class, TestSpringDemoConfig.class})
@DirtiesContext(classMode =  DirtiesContext.ClassMode.AFTER_CLASS)
@AutoConfigureWireMock(port = 0)
@ActiveProfiles("test")
@EmbeddedKafka(controlledShutdown = true, topics = {"payment-sent"})
public class PaymentIntegrationTest {
    private final static String SEND_PAYMENT_TOPIC = "send-payment";
    private final static String PAYMENT_SENT_TOPIC = "payment-sent";

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
//        @KafkaListener(groupId = "PaymentIntegrationTest", topics = "payment-sent", containerFactory = "testKafkaListenerContainerFactory", autoStartup = "true")
//        void receive(@Payload final PaymentSent payload) {
//            log.debug("KafkaTestListener - Received payment with Id: " + payload.getPaymentId());
//            counter.incrementAndGet();
//        }
//    }

    @BeforeEach
    public void setUp() throws Exception {
        this.registry.getListenerContainers().stream().forEach(container ->
                ContainerTestUtils.waitForAssignment(container, this.embeddedKafkaBroker.getPartitionsPerTopic()));
        this.testReceiver.counter.set(0);

        WireMock.reset();
        WireMock.resetAllRequests();
        WireMock.resetAllScenarios();
        WireMock.resetToDefault();

        this.registerSchema(1, SEND_PAYMENT_TOPIC, SendPayment.getClassSchema().toString());
        this.registerSchema(2, PAYMENT_SENT_TOPIC, PaymentSent.getClassSchema().toString());
    }
    private void registerSchema(int schemaId, String topic, String schema) throws Exception {
        stubFor(post(urlPathMatching("/subjects/" + topic + "-value"))
                .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody("{\"id\":" + schemaId + "}")));

        final SchemaString schemaString = new SchemaString(schema);
        stubFor(get(urlPathMatching("/schemas/ids/" + schemaId))
                .willReturn(aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(schemaString.toJson())));
    }

    @Test
    public void testSendPaymentViaRest() {
        int totalMessages = 10;

        for(int i = 0; i < totalMessages; i++) {
            String paymentId = UUID.randomUUID().toString();

            SendPaymentRequest request = TestData.buildSendPaymentRequest(paymentId);
            this.sendRequestViaRest(request);
        }

        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiver.counter::get, is(totalMessages));
    }

    @Test
    public void testSendPaymentViaKafka() throws Exception {
        int totalMessages = 10;

        for(int i = 0; i < totalMessages; i++) {
            String paymentId = UUID.randomUUID().toString();

            SendPayment command = TestData.buildSendPayment(paymentId);
            this.sendCommandViaKafka(command);
        }

        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(this.testReceiver.counter::get, is(totalMessages));
    }

    private void sendRequestViaRest(SendPaymentRequest request) {
        ResponseEntity<String> response = restTemplate.postForEntity("/v1/payments/send", request, String.class);
        assertThat(response.getStatusCode(), equalTo(HttpStatus.OK));
        assertThat(response.getBody(), is(request.getPaymentId()));
    }

    private void sendCommandViaKafka(SendPayment command) throws Exception {
        final ProducerRecord<Long, String> record = new ProducerRecord(SEND_PAYMENT_TOPIC, null, command.getPaymentId(), command);
        this.testKafkaTemplate.send(record).get();
    }
}
