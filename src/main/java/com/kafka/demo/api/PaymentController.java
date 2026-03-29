package com.kafka.demo.api;

import com.kafka.demo.dto.PaymentRequest;
import com.kafka.demo.producer.PaymentEventProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentEventProducer paymentEventProducer;

    @PostMapping
    public ResponseEntity<Map<String, String>> createPayment(
            @Valid @RequestBody PaymentRequest request) {

        paymentEventProducer.publishPaymentRequested(request);

        return ResponseEntity.accepted().body(Map.of(
                "paymentId", request.paymentId(),
                "status", "PAYMENT_REQUESTED",
                "message", "Ödeme olayı Kafka'ya bırakıldı"
        ));
    }

}
