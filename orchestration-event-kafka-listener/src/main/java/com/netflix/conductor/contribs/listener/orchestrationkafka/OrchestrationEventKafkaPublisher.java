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

import java.util.Set;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.contribs.tasks.kafka.KafkaProducerManager;
import com.netflix.conductor.core.listener.WorkflowStatusListener;
import com.netflix.conductor.model.WorkflowModel;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Publishes workflow lifecycle events, packed as {@link ConductorEvent}, to a Kafka topic. */
public class OrchestrationEventKafkaPublisher implements WorkflowStatusListener {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(OrchestrationEventKafkaPublisher.class);

    private final OrchestrationEventKafkaPublisherProperties properties;
    private final ObjectMapper objectMapper;
    private final Producer<String, String> producer;
    private final ConductorEventFactory eventFactory;
    private final Set<WorkflowEventType> subscribedEvents;

    /**
     * Producer comes from the shared {@link KafkaProducerManager}, so this listener picks up the
     * same globally-configured broker/SSL settings as the kafka-publish task, layering its own
     * {@link OrchestrationEventKafkaPublisherProperties#getProducer() producer} overrides on top.
     * The manager owns the producer's lifecycle (shared cache, eviction), so this listener does not
     * close it itself.
     */
    public OrchestrationEventKafkaPublisher(
            OrchestrationEventKafkaPublisherProperties properties,
            ObjectMapper objectMapper,
            KafkaProducerManager kafkaProducerManager) {
        this(
                properties,
                objectMapper,
                kafkaProducerManager.getProducerForOverrides(properties.toProducerConfig()));
    }

    /** Visible for tests, so a {@code MockProducer} can stand in for a real broker connection. */
    OrchestrationEventKafkaPublisher(
            OrchestrationEventKafkaPublisherProperties properties,
            ObjectMapper objectMapper,
            Producer<String, String> producer) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.producer = producer;
        this.eventFactory = new ConductorEventFactory();
        this.subscribedEvents = Set.copyOf(properties.getSubscribedEvents());
    }

    @Override
    public void onWorkflowStarted(WorkflowModel workflow) {
        publishEvent(WorkflowEventType.STARTED, workflow);
    }

    @Override
    public void onWorkflowRestarted(WorkflowModel workflow) {
        publishEvent(WorkflowEventType.RESTARTED, workflow);
    }

    @Override
    public void onWorkflowRerun(WorkflowModel workflow) {
        publishEvent(WorkflowEventType.RERAN, workflow);
    }

    @Override
    public void onWorkflowRetried(WorkflowModel workflow) {
        publishEvent(WorkflowEventType.RETRIED, workflow);
    }

    @Override
    public void onWorkflowPaused(WorkflowModel workflow) {
        publishEvent(WorkflowEventType.PAUSED, workflow);
    }

    @Override
    public void onWorkflowResumed(WorkflowModel workflow) {
        publishEvent(WorkflowEventType.RESUMED, workflow);
    }

    @Override
    public void onWorkflowCompleted(WorkflowModel workflow) {
        publishEvent(WorkflowEventType.COMPLETED, workflow);
    }

    @Override
    public void onWorkflowTerminated(WorkflowModel workflow) {
        publishEvent(WorkflowEventType.TERMINATED, workflow);
    }

    @Override
    public void onWorkflowFinalized(WorkflowModel workflow) {
        publishEvent(WorkflowEventType.FINALIZED, workflow);
    }

    private void publishEvent(WorkflowEventType eventType, WorkflowModel workflow) {
        if (!subscribedEvents.contains(eventType)) {
            return;
        }
        try {
            ConductorEvent message = eventFactory.buildMessage(eventType, workflow);
            String jsonPayload = objectMapper.writeValueAsString(message);
            String topic =
                    properties
                            .getEventTopics()
                            .getOrDefault(eventType, properties.getDefaultTopic());

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, workflow.getWorkflowId(), jsonPayload);

            producer.send(
                    record,
                    (metadata, exception) -> {
                        if (exception != null) {
                            LOGGER.error(
                                    "Failed to publish workflow event {} for workflow {}",
                                    eventType,
                                    workflow.getWorkflowId(),
                                    exception);
                        } else {
                            LOGGER.debug(
                                    "Published event {} for workflow {} to topic {} partition {} offset {}",
                                    eventType,
                                    workflow.getWorkflowId(),
                                    metadata.topic(),
                                    metadata.partition(),
                                    metadata.offset());
                        }
                    });
        } catch (Exception e) {
            LOGGER.error(
                    "Unexpected error publishing event {} for workflow {}: {}",
                    eventType,
                    workflow.getWorkflowId(),
                    e.getMessage(),
                    e);
        }
    }
}
