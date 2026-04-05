package com.kafka.demo.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public record PaymentRequest(
        @NotBlank String paymentId,
        @NotBlank String customerId,
        @NotNull BigDecimal amount,
        @NotBlank String currency,
        String scenario
) {
}
