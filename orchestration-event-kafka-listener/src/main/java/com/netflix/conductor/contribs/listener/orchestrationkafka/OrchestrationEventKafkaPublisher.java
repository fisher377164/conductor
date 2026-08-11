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

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.contribs.tasks.kafka.KafkaProducerManager;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Delivers a {@link ConductorEvent} to Kafka: serializes it and sends it, logging the outcome.
 * Shared by {@link OrchestrationWorkflowStatusListener} and {@link OrchestrationTaskStatusListener}
 * so the send/log/error-handling logic isn't duplicated between the two listener types; it has no
 * notion of workflows or tasks itself, only of publishing a keyed record to a topic.
 */
public class OrchestrationEventKafkaPublisher {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(OrchestrationEventKafkaPublisher.class);

    private final ObjectMapper objectMapper;
    private final Producer<String, String> producer;

    /**
     * Producer comes from the shared {@link KafkaProducerManager}, so callers pick up the same
     * globally-configured broker/SSL settings as the kafka-publish task, layering their own {@link
     * OrchestrationEventKafkaPublisherProperties#getProducer() producer} overrides on top. The
     * manager owns the producer's lifecycle (shared cache, eviction), so this class does not close
     * it itself.
     */
    public OrchestrationEventKafkaPublisher(
            OrchestrationEventKafkaPublisherProperties properties,
            ObjectMapper objectMapper,
            KafkaProducerManager kafkaProducerManager) {
        this(
                objectMapper,
                kafkaProducerManager.getProducerForOverrides(properties.toProducerConfig()));
    }

    /** Visible for tests, so a {@code MockProducer} can stand in for a real broker connection. */
    OrchestrationEventKafkaPublisher(ObjectMapper objectMapper, Producer<String, String> producer) {
        this.objectMapper = objectMapper;
        this.producer = producer;
    }

    /**
     * Serializes {@code message} and sends it to {@code topic} keyed by {@code key}. {@code
     * eventType} and {@code key} are only used for logging; failures are caught and logged, never
     * thrown, so a Kafka outage cannot fail the workflow/task transition that triggered the event.
     */
    public void publish(String topic, String key, String eventType, ConductorEvent message) {
        try {
            String jsonPayload = objectMapper.writeValueAsString(message);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, jsonPayload);

            producer.send(
                    record,
                    (metadata, exception) -> {
                        if (exception != null) {
                            LOGGER.error(
                                    "Failed to publish event {} for key {}",
                                    eventType,
                                    key,
                                    exception);
                        } else {
                            LOGGER.debug(
                                    "Published event {} for key {} to topic {} partition {} offset {}",
                                    eventType,
                                    key,
                                    metadata.topic(),
                                    metadata.partition(),
                                    metadata.offset());
                        }
                    });
        } catch (Exception e) {
            LOGGER.error(
                    "Unexpected error publishing event {} for key {}: {}",
                    eventType,
                    key,
                    e.getMessage(),
                    e);
        }
    }
}
