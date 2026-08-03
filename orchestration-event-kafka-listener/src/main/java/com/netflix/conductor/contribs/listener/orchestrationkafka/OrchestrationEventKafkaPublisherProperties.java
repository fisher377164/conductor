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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

import com.netflix.conductor.core.listener.WorkflowStatusListener.WorkflowEventType;

@ConfigurationProperties(prefix = "conductor.workflow-status-listener.orchestration-kafka")
public class OrchestrationEventKafkaPublisherProperties {

    /** Raw Kafka producer properties, e.g. producer[bootstrap.servers]=localhost:9092. */
    private Map<String, Object> producer = new HashMap<>();

    /** Kafka topic events are published to when no {@link #eventTopics} override applies. */
    private String defaultTopic = "orchestration-workflow-events";

    /** Per-event-type topic overrides; falls back to {@link #defaultTopic} when absent. */
    private Map<WorkflowEventType, String> eventTopics = new EnumMap<>(WorkflowEventType.class);

    /**
     * Workflow lifecycle events to publish. Defaults to all events supported by {@link
     * WorkflowEventType}.
     */
    private List<WorkflowEventType> subscribedEvents =
            new ArrayList<>(List.of(WorkflowEventType.values()));

    public Map<String, Object> getProducer() {
        return producer;
    }

    public void setProducer(Map<String, Object> producer) {
        this.producer = producer;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }

    public Map<WorkflowEventType, String> getEventTopics() {
        return eventTopics;
    }

    public void setEventTopics(Map<WorkflowEventType, String> eventTopics) {
        this.eventTopics = eventTopics;
    }

    public List<WorkflowEventType> getSubscribedEvents() {
        return subscribedEvents;
    }

    public void setSubscribedEvents(List<WorkflowEventType> subscribedEvents) {
        this.subscribedEvents = subscribedEvents;
    }

    /**
     * Maps {@link #producer} against {@link ProducerConfig} keys. These are overrides layered on
     * top of {@code KafkaProducerManager}'s own base producer config (bootstrap servers, SSL/SASL,
     * etc.) by {@code OrchestrationEventKafkaPublisher}; anything not set here falls through to
     * that shared, globally-configured base.
     */
    public Map<String, Object> toProducerConfig() {
        Map<String, Object> config = new HashMap<>();
        for (String key : ProducerConfig.configNames()) {
            if (producer.containsKey(key)) {
                config.put(key, producer.get(key));
            }
        }
        return config;
    }
}
