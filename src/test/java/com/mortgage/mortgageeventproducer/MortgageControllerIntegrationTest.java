package com.mortgage.mortgageeventproducer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mortgage.mortgageeventproducer.model.Address;
import com.mortgage.mortgageeventproducer.model.MortgageEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"mortgage-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
public class MortgageControllerIntegrationTest {

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    ObjectMapper objectMapper;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void testProduceAndConsumeMortgageEvent() throws JsonProcessingException, InterruptedException {

        MortgageEvent mortgageEvent = createMortgageEvent();
        System.out.println("mortgageEvent : " + objectMapper.writeValueAsString(mortgageEvent));
        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<MortgageEvent> request = new HttpEntity<>(mortgageEvent, headers);

        ResponseEntity<MortgageEvent> responseEntity = restTemplate.exchange("/v1/mortgageevent", HttpMethod.POST, request, MortgageEvent.class);

        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

        ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer);
        Thread.sleep(5000);
        assert consumerRecords.count() == 1;
        consumerRecords.forEach(record -> {
            MortgageEvent receivedMortgageEvent = null;
            try {
                receivedMortgageEvent = objectMapper.readValue(record.value(), MortgageEvent.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            assertEquals(mortgageEvent, receivedMortgageEvent);

        });
    }

    private static MortgageEvent createMortgageEvent() {
        return MortgageEvent.builder().mortgageId(12345)
                .customerAddress(Address.builder().addressLine1("").addressLine2("").addressLine3("").build())
                .mortgageAmount(new BigDecimal("1000.00")).build();
    }
}
