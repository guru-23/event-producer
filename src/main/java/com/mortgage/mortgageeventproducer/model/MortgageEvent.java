package com.mortgage.mortgageeventproducer.model;


import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class MortgageEvent {

    private Integer mortgageId;

    private BigDecimal mortgageAmount;

    private Address customerAddress;
}
