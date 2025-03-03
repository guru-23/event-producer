package com.mortgage.mortgageeventproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mortgage.mortgageeventproducer.model.MortgageEvent;
import com.mortgage.mortgageeventproducer.producer.MortgageEventProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController

public class MortgageController {

    @Autowired
    MortgageEventProducer mortgageEventProducer;

    @PostMapping("/v1/mortgageevent")
    public ResponseEntity<?> postEvent(@RequestBody  MortgageEvent mortgageEvent) throws JsonProcessingException {

        mortgageEventProducer.sendEvent(mortgageEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(mortgageEvent);
    }
}
