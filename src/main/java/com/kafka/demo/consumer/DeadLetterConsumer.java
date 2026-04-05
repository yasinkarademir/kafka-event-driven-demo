package com.kafka.demo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DeadLetterConsumer {

    @KafkaListener(
            topics = {
                    "${app.topics.payment-requested-dlt}",
                    "${app.topics.payment-processed-dlt}"
            },
            groupId = "dlt-group"
    )
    public void consume(
            String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String dltTopic,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
            @Header(value = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false) Long originalOffset,
            @Header(value = KafkaHeaders.DLT_EXCEPTION_FQCN, required = false) String exceptionClass,
            @Header(value = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage) {

        log.error("DLT consume -> dltTopic={}, originalTopic={}, originalOffset={}, exceptionClass={}, exceptionMessage={}",
                dltTopic, originalTopic, originalOffset, exceptionClass, exceptionMessage);
        log.error("DLT payload -> {}", message);
    }
}
