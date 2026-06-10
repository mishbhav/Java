package com.tutor.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic tutorTopic() {
        return TopicBuilder.name("tutor-messages").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic analyticsTopic() {
        return TopicBuilder.name("tutor-analytics").partitions(3).replicas(1).build();
    }
}
