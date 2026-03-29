package com.kafka.demo.producer;

import com.kafka.demo.dto.PaymentRequest;
import com.kafka.demo.event.PaymentProcessedEvent;
import com.kafka.demo.event.PaymentRequestedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class PaymentEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.topics.payment-requested}")
    private String paymentRequestedTopic;

    @Value("${app.topics.payment-processed}")
    private String paymentProcessedTopic;

    public void publishPaymentRequested(PaymentRequest request) {

        PaymentRequestedEvent event = new PaymentRequestedEvent(
                request.paymentId(),
                request.customerId(),
                request.amount(),
                request.currency(),
                Instant.now()
        );

        kafkaTemplate.send(paymentRequestedTopic, request.paymentId(), toJson(event));

    }

    public void publishPaymentProcessed(PaymentProcessedEvent event) {
        kafkaTemplate.send(paymentProcessedTopic, event.paymentId(), toJson(event));
    }

    private String toJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            throw new RuntimeException("JSON serialize edilemedi", e);
        }
    }

}
