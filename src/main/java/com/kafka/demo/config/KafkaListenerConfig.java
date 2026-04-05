package com.kafka.demo.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class KafkaListenerConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.topics.payment-requested}") String paymentRequestedTopic,
            @Value("${app.topics.payment-processed}") String paymentProcessedTopic,
            @Value("${app.topics.payment-requested-dlt}") String paymentRequestedDltTopic,
            @Value("${app.topics.payment-processed-dlt}") String paymentProcessedDltTopic,
            @Value("${app.retry.max-attempts}") long maxAttempts,
            @Value("${app.retry.backoff-ms}") long backOffMs) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(resolveDltTopic(
                        record.topic(),
                        paymentRequestedTopic,
                        paymentProcessedTopic,
                        paymentRequestedDltTopic,
                        paymentProcessedDltTopic
                ), record.partition())
        );

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(backOffMs, Math.max(0, maxAttempts - 1))
        );

        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    private String resolveDltTopic(
            String sourceTopic,
            String paymentRequestedTopic,
            String paymentProcessedTopic,
            String paymentRequestedDltTopic,
            String paymentProcessedDltTopic) {

        if (paymentRequestedTopic.equals(sourceTopic)) {
            return paymentRequestedDltTopic;
        }

        if (paymentProcessedTopic.equals(sourceTopic)) {
            return paymentProcessedDltTopic;
        }

        String fallback = sourceTopic + ".dlt";
        log.warn("DLT mapping bulunamadi, varsayilan topic kullaniliyor -> {}", fallback);
        return fallback;
    }
}
