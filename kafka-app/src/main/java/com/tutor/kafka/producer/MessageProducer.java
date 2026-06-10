package com.tutor.kafka.producer;

import com.tutor.kafka.model.UserMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProducer {

    private final KafkaTemplate<String, UserMessage> kafkaTemplate;

    public void sendMessage(UserMessage message) {
        log.info("→ Attempting to produce message: {}", message.getId());

        // 1. Kick off the asynchronous send action
        CompletableFuture<SendResult<String, UserMessage>> future = 
                kafkaTemplate.send("tutor-messages", message.getId(), message);

        // 2. Attach callbacks to execute when Kafka responds
        future.whenComplete((result, exception) -> {
            if (exception == null) {
                // SUCCESS SCENARIO
                log.info("✔ Successfully published to Kafka!");
                log.info("  Topic:     {}", result.getRecordMetadata().topic());
                log.info("  Partition: {}", result.getRecordMetadata().partition());
                log.info("  Offset:    {}", result.getRecordMetadata().offset());
            } else {
                // FAILURE SCENARIO (e.g., network timeout, broker offline)
                log.error("❌ Failed to send message to Kafka!", exception);
                
                // Production tip: Here you would write fallback code 
                // like saving the message to a database for a retry later.
            }
        });
    }
}
