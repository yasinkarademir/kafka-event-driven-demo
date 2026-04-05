package com.kafka.demo.consumer;

import com.kafka.demo.event.PaymentProcessedEvent;
import com.kafka.demo.event.PaymentRequestedEvent;
import com.kafka.demo.producer.PaymentEventProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class AuthorizationConsumer {

    private final ObjectMapper objectMapper;
    private final PaymentEventProducer paymentEventProducer;

    @KafkaListener(topics = "${app.topics.payment-requested}", groupId = "authorization-group")
    public void consume(
            String message,
            @Header(value = KafkaHeaders.DELIVERY_ATTEMPT, required = false) Integer deliveryAttempt) {

        PaymentRequestedEvent event = fromJson(message, PaymentRequestedEvent.class);

        log.info("Authorization consume -> paymentId={}, attempt={}, scenario={}",
                event.paymentId(), normalizeAttempt(deliveryAttempt), event.scenario());

        if ("FAIL_AUTH".equalsIgnoreCase(event.scenario())) {
            throw new IllegalStateException("Authorization tarafinda bilincli hata tetiklendi");
        }

        boolean approved = event.amount().compareTo(new BigDecimal("10000")) <= 0;

        PaymentProcessedEvent processedEvent = new PaymentProcessedEvent(
                event.paymentId(),
                event.customerId(),
                event.amount(),
                event.currency(),
                event.scenario(),
                approved ? "APPROVED" : "REJECTED",
                approved ? "Mock bank approval" : "Manual review required",
                Instant.now()
        );

        log.info("Authorization sonucu -> paymentId={}, status={}", processedEvent.paymentId(), processedEvent.status());

        paymentEventProducer.publishPaymentProcessed(processedEvent);
    }

    private <T> T fromJson(String message, Class<T> clazz) {
        try {
            return objectMapper.readValue(message, clazz);
        } catch (Exception e) {
            throw new RuntimeException("JSON parse edilemedi", e);
        }
    }

    private int normalizeAttempt(Integer deliveryAttempt) {
        return deliveryAttempt == null ? 1 : deliveryAttempt;
    }
}
