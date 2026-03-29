package com.kafka.demo.consumer;

import com.kafka.demo.event.PaymentProcessedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class LedgerConsumer {

    private final ObjectMapper objectMapper;


    @KafkaListener(topics = "${app.topics.payment-processed}", groupId = "ledger-group")
    public void consume(String message) {
        PaymentProcessedEvent event = fromJson(message, PaymentProcessedEvent.class);

        log.info("Ledger/notification simülasyonu -> paymentId={}, status={}, amount={}, currency={}",
                event.paymentId(), event.status(), event.amount(), event.currency());
    }


    private <T> T fromJson(String message, Class<T> clazz) {
        try {
            return objectMapper.readValue(message, clazz);
        } catch (Exception e) {
            throw new RuntimeException("JSON parse edilemedi", e);
        }
    }
}
