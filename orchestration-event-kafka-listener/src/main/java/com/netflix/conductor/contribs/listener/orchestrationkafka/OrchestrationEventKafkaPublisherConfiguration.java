/*
 * Copyright 2026 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.contribs.listener.orchestrationkafka;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.netflix.conductor.contribs.tasks.kafka.KafkaProducerManager;
import com.netflix.conductor.core.listener.TaskStatusListener;
import com.netflix.conductor.core.listener.WorkflowStatusListener;

import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
@EnableConfigurationProperties(OrchestrationEventKafkaPublisherProperties.class)
public class OrchestrationEventKafkaPublisherConfiguration {

    @Bean
    @ConditionalOnProperty(
            name = "conductor.workflow-status-listener.type",
            havingValue = "orchestration-kafka")
    public WorkflowStatusListener getWorkflowStatusListener(
            OrchestrationEventKafkaPublisherProperties properties,
            ObjectMapper objectMapper,
            KafkaProducerManager kafkaProducerManager) {
        return new OrchestrationWorkflowStatusListener(
                properties, objectMapper, kafkaProducerManager);
    }

    /**
     * Independently gated by {@code conductor.task-status-listener.type}, so task tracking can be
     * turned on without affecting the {@link #getWorkflowStatusListener} bean above (or vice
     * versa).
     */
    @Bean
    @ConditionalOnProperty(
            name = "conductor.task-status-listener.type",
            havingValue = "orchestration-kafka")
    public TaskStatusListener getTaskStatusListener(
            OrchestrationEventKafkaPublisherProperties properties,
            ObjectMapper objectMapper,
            KafkaProducerManager kafkaProducerManager) {
        return new OrchestrationTaskStatusListener(properties, objectMapper, kafkaProducerManager);
    }
}
