package com.mortgage.mortgageeventproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mortgage.mortgageeventproducer.model.MortgageEvent;
import com.mortgage.mortgageeventproducer.repository.MortgageEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class MortgageEventProducer {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;

    @Autowired
    private MortgageEventRepository eventRepository;

    @Value("${spring.kafka.topic}")
    public String eventTopic;

    @Value("${topics.retry}")
    public String retryTopic;

    public CompletableFuture<SendResult<Integer, String>> sendEvent(MortgageEvent mortgageEvent) throws JsonProcessingException {

        Integer key = mortgageEvent.getMortgageId();
        String value = objectMapper.writeValueAsString(mortgageEvent);

        ProducerRecord<Integer, String> producerRecord = getProducerRecord(eventTopic, key, value);
        CompletableFuture<SendResult<Integer, String>> completableFuture = kafkaTemplate.send(producerRecord);
        return completableFuture
                .whenComplete((sendResult, throwable) -> {
                    if (throwable != null) {
                        handleFailureWriteToTopic(key, value, throwable);
                    } else {
                        handleSuccess(key, value, sendResult);
                    }
                });
    }

    private void handleFailureWriteToTable(Integer key, String value, Throwable ex)  {
        log.error("Error Sending the Message and the exception is {}", ex.getMessage());

        MortgageEvent event = null;
        try {
            event = objectMapper.readValue(value, MortgageEvent.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        save(event);
    }


    private void handleFailureWriteToTopic(Integer key, String value, Throwable ex) {
        log.error("Error Sending the Message and the exception is {}", ex.getMessage());
        ProducerRecord<Integer, String> producerRecord = getProducerRecord(retryTopic, key, value);
        CompletableFuture<SendResult<Integer, String>> completableFuture = kafkaTemplate.send(producerRecord);
        completableFuture
                .whenComplete((sendResult, throwable) -> {
                    if (throwable != null) {
                        log.error("Error Sending the Retry Message and the exception is {}", throwable.getMessage());
                    } else {
                        handleSuccess(key, value, sendResult);
                    }
                });
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message Sent SuccessFully for the key : {} and the value is {}", key, value);
    }

    private void save(MortgageEvent mortgageEvent) {
        //Saves message to database
        eventRepository.save(mortgageEvent);
    }

    private ProducerRecord<Integer, String> getProducerRecord(String topic, Integer key, String value) {
        return new ProducerRecord<>(topic, key, value);
    }
}
