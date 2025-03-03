package com.mortgage.mortgageeventproducer.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Address {

    private String addressLine1;

    private String addressLine2;

    private String addressLine3;

    private String postcode;

}

