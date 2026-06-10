package com.tutor.kafka.config;

import lombok.extern.slf4j.Slf4j;
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
public class KafkaConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            KafkaTemplate<String, Object> kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // 1. Define where to send failed messages (defaults to: original-topic-name.DLT)
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);

        // 2. Define the retry policy: Try 3 times total, with a 2-second delay between attempts [1]
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                recoverer, 
                new FixedBackOff(2000L, 2) // 2000ms delay, 2 retries (total 3 attempts) [1]
        );

        // Log a warning in your console whenever a retry attempt happens
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.warn("⚠️ Retry attempt #{} failed for message key: {}", deliveryAttempt, record.key());
        });

        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
