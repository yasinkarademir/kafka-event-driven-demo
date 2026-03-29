package com.kafka.demo.event;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentProcessedEvent(
        String paymentId,
        String customerId,
        BigDecimal amount,
        String currency,
        String status,
        String reason,
        Instant processedAt
) {
}