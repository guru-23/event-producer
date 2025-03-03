package com.mortgage.mortgageeventproducer.repository;


import com.mortgage.mortgageeventproducer.model.MortgageEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MortgageEventRepositoryImpl implements MortgageEventRepository{
    @Override
    public void save(MortgageEvent mortgageEvent) {
        log.info("Successfully Persisted the Event {} ", mortgageEvent);
    }
}
