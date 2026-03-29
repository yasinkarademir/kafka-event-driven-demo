package com.kafka.demo.event;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentRequestedEvent(
        String paymentId,
        String customerId,
        BigDecimal amount,
        String currency,
        Instant createdAt
) {}