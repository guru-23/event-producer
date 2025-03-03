package com.mortgage.mortgageeventproducer.repository;

import com.mortgage.mortgageeventproducer.model.MortgageEvent;

public interface MortgageEventRepository {

    void save(MortgageEvent mortgageEvent);
}
