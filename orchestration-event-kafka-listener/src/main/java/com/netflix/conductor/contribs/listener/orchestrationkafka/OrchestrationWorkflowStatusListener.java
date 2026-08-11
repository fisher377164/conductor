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

import com.netflix.conductor.contribs.tasks.kafka.KafkaProducerManager;
import com.netflix.conductor.core.listener.WorkflowStatusListener;
import com.netflix.conductor.model.WorkflowModel;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Publishes workflow lifecycle events, packed as {@link ConductorEvent}, to a Kafka topic. */
public class OrchestrationWorkflowStatusListener implements WorkflowStatusListener {

    private final OrchestrationEventKafkaPublisherProperties properties;
    private final ConductorEventFactory eventFactory;
    private final OrchestrationEventKafkaPublisher publisher;
    private final Set<WorkflowEventType> subscribedEvents;

    public OrchestrationWorkflowStatusListener(
            OrchestrationEventKafkaPublisherProperties properties,
            ObjectMapper objectMapper,
            KafkaProducerManager kafkaProducerManager) {
        this(
                properties,
                new OrchestrationEventKafkaPublisher(
                        properties, objectMapper, kafkaProducerManager));
    }

    /**
     * Visible for tests, so a publisher backed by a {@code MockProducer} can be injected directly.
     */
    OrchestrationWorkflowStatusListener(
            OrchestrationEventKafkaPublisherProperties properties,
            OrchestrationEventKafkaPublisher publisher) {
        this.properties = properties;
        this.eventFactory = new ConductorEventFactory();
        this.publisher = publisher;
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
        ConductorEvent message = eventFactory.buildMessage(eventType, workflow);
        String topic =
                properties.getEventTopics().getOrDefault(eventType, properties.getDefaultTopic());
        publisher.publish(topic, workflow.getWorkflowId(), eventType.name(), message);
    }
}
