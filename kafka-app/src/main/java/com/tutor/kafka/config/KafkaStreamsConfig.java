package com.tutor.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tutor.kafka.model.UserMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Slf4j
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean
    public KStream<String, String> kStreamTopology(StreamsBuilder streamsBuilder) {
        ObjectMapper objectMapper = new ObjectMapper();

        // 1. STAGE: Consume incoming raw data as a KStream from the source topic
        KStream<String, String> sourceStream = streamsBuilder.stream(
                "tutor-messages", 
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // 2. STAGE: Apply real-time transformations
        KStream<String, String> transformedStream = sourceStream
                .mapValues(rawJson -> {
                    try {
                        // Deserialize JSON to mutate data
                        UserMessage message = objectMapper.readValue(rawJson, UserMessage.class);
                        
                        log.info("⚡ Streams Pipeline Intercepted: '{}'", message.getMessage());
                        
                        // Perform enrichment / transformation logic
                        String upperText = message.getMessage().toUpperCase();
                        
                        // Return modified payload format
                        return "{\"processedMessage\":\"" + upperText + "\", \"status\":\"ENRICHED\"}";
                    } catch (Exception e) {
                        log.error("❌ Streams pipeline failed parsing record", e);
                        return "{\"status\":\"CORRUPTED\"}";
                    }
                });

        // 3. STAGE: Route the refined, processed event stream out to the target topic
        transformedStream.to("tutor-analytics", Produced.with(Serdes.String(), Serdes.String()));

        return sourceStream;
    }
}
