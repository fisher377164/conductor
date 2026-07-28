package com.netflix.conductor.contribs.listener.orchestrationkafka;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.netflix.conductor.core.listener.WorkflowStatusListener;

import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
@EnableConfigurationProperties(OrchestrationEventKafkaPublisherProperties.class)
@ConditionalOnProperty(name = "conductor.workflow-status-listener.type", havingValue = "orchestration-kafka")
public class OrchestrationEventKafkaPublisherConfiguration {

    @Bean
    public WorkflowStatusListener getWorkflowStatusListener(
            OrchestrationEventKafkaPublisherProperties properties, ObjectMapper objectMapper) {
        return new OrchestrationEventKafkaPublisher(properties, objectMapper);
    }
}
