package com.tutor.kafka.consumer;

import com.tutor.kafka.model.UserMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import com.tutor.kafka.model.UserMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;

@Slf4j
@Service
public class MessageConsumer  implements ConsumerSeekAware{

	/*
	 * @KafkaListener(topics = "tutor-messages", groupId = "tutor-group") public
	 * void consume(UserMessage message,
	 * 
	 * @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
	 * 
	 * @Header(KafkaHeaders.OFFSET) long offset) {
	 * 
	 * log.info("← Received message: '{}' from Partition: {}, Offset: {}",
	 * message.getMessage(), partition, offset);
	 * 
	 * // Simulate a system crash if a bad payload arrives if
	 * (message.getMessage().toLowerCase().contains("poison")) {
	 * log.error("💥 Critical error encountered while processing message ID: {}",
	 * message.getId()); throw new
	 * RuntimeException("Simulated business logic failure!"); }
	 * 
	 * log.info("✔ Message processed successfully."); }
	 * 
	 * // 3. The Dead Letter Queue Listener // By default, Spring appends ".DLT"
	 * (Dead Letter Topic) to your original topic name [1]
	 * 
	 * @KafkaListener(topics = "tutor-messages.DLT", groupId = "tutor-dlq-group")
	 * public void consumeDeadLetterQueue(UserMessage failedMessage,
	 * 
	 * @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage) {
	 * 
	 * log.error("🚨 ALERT: Message moved to DEAD LETTER QUEUE (DLQ)!");
	 * log.error("  Failed Message Content: {}", failedMessage.getMessage());
	 * log.error("  Reason for Failure:      {}", exceptionMessage); }
	 */
	
	/*
	 * 
	 * // 1. FILTERING: Only process messages where text length is greater than 3
	 * characters
	 * 
	 * @KafkaListener( topics = "tutor-messages", groupId = "tutor-group", filter =
	 * "#{record.value().message.length() > 3}" ) public void consume(UserMessage
	 * message,
	 * 
	 * @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
	 * 
	 * @Header(KafkaHeaders.OFFSET) long offset, Acknowledgment ack) { // Injecting
	 * the manual commit handle
	 * 
	 * log.info("← Received Message: '{}' (Partition: {}, Offset: {})",
	 * message.getMessage(), partition, offset);
	 * 
	 * try { // 2. SIMULATE BUSINESS LOGIC PROCESSING
	 * log.info("Processing business logic for message ID: {}...", message.getId());
	 * Thread.sleep(1000); // Simulating database write or external API call
	 * 
	 * // 3. SUCCESS: Manually commit the offset to Kafka ack.acknowledge();
	 * log.info("✔ Offset committed successfully for message: {}", message.getId());
	 * 
	 * } catch (Exception e) {
	 * log.error("❌ Business logic failed! Offset NOT committed.", e); // By NOT
	 * calling ack.acknowledge(), Kafka keeps the message marked as unread // for
	 * this consumer group if the instance crashes or restarts. } }
	 * 
	 * @KafkaListener(topics = "tutor-messages.DLT", groupId = "tutor-dlq-group")
	 * public void consumeDeadLetterQueue(UserMessage failedMessage,
	 * 
	 * @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage) {
	 * log.error("🚨 DLQ Alert: {}", failedMessage.getMessage()); }
	 */

	


	   @KafkaListener(
	            topics = "tutor-messages", 
	            groupId = "tutor-group",
	            filter = "#{record.value().message.length() > 3}"
	    )
	    public void consume(UserMessage message,
	                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
	                        @Header(KafkaHeaders.OFFSET) long offset,
	                        Acknowledgment ack) {
	        
	        log.info("← Processing Message: '{}' from Partition: {}", message.getMessage(), partition);
	        ack.acknowledge();
	    }

	    // INTERCEPTING REBALANCE EVENTS: Called when Kafka assigns partitions to this instance
	    @Override
	    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
	        log.info("🔄 REBALANCE EVENT: Partitions assigned to this worker application instance:");
	        assignments.keySet().forEach(tp -> 
	            log.info("   ↳ Subscribed to Topic: {}, Partition ID: {}", tp.topic(), tp.partition())
	        );
	    }

	    // INTERCEPTING REBALANCE EVENTS: Called when Kafka revokes partitions from this instance
	    @Override
	    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
	        log.warn("⚠️ REBALANCE EVENT: Partitions REVOKED from this worker instance:");
	        partitions.forEach(tp -> 
	            log.warn("   ↳ Lost ownership of Partition ID: {}", tp.partition())
	        );
	        // Production tip: This is where you quickly commit database transactions 
	        // before another machine takes over this partition's stream!
	    }

	    @Override
	    public void registerSeekCallback(ConsumerSeekCallback callback) {}

	    @Override
	    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {}

	    @KafkaListener(topics = "tutor-analytics", groupId = "tutor-analytics-group")
	    public void consumeAnalytics(String processedJson) {
	        log.info("📊 METRICS CAPTURED from tutor-analytics topic: {}", processedJson);
	    }

}