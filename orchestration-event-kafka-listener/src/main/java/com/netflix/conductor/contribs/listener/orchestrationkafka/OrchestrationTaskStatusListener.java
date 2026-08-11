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

import com.netflix.conductor.contribs.tasks.kafka.KafkaProducerManager;
import com.netflix.conductor.core.listener.TaskStatusListener;
import com.netflix.conductor.model.TaskModel;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Publishes task status changes, packed as {@link ConductorEvent}, to a Kafka topic — the same
 * topic {@link OrchestrationWorkflowStatusListener} uses, see {@code default-topic} in {@link
 * OrchestrationEventKafkaPublisherProperties}.
 */
public class OrchestrationTaskStatusListener implements TaskStatusListener {

    private final OrchestrationEventKafkaPublisherProperties properties;
    private final ConductorEventFactory eventFactory;
    private final OrchestrationEventKafkaPublisher publisher;

    public OrchestrationTaskStatusListener(
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
    OrchestrationTaskStatusListener(
            OrchestrationEventKafkaPublisherProperties properties,
            OrchestrationEventKafkaPublisher publisher) {
        this.properties = properties;
        this.eventFactory = new ConductorEventFactory();
        this.publisher = publisher;
    }

    @Override
    public void onTaskScheduled(TaskModel task) {
        publishTaskEvent(TaskModel.Status.SCHEDULED, task);
    }

    @Override
    public void onTaskInProgress(TaskModel task) {
        publishTaskEvent(TaskModel.Status.IN_PROGRESS, task);
    }

    @Override
    public void onTaskCanceled(TaskModel task) {
        publishTaskEvent(TaskModel.Status.CANCELED, task);
    }

    @Override
    public void onTaskFailed(TaskModel task) {
        publishTaskEvent(TaskModel.Status.FAILED, task);
    }

    @Override
    public void onTaskFailedWithTerminalError(TaskModel task) {
        publishTaskEvent(TaskModel.Status.FAILED_WITH_TERMINAL_ERROR, task);
    }

    @Override
    public void onTaskCompleted(TaskModel task) {
        publishTaskEvent(TaskModel.Status.COMPLETED, task);
    }

    @Override
    public void onTaskCompletedWithErrors(TaskModel task) {
        publishTaskEvent(TaskModel.Status.COMPLETED_WITH_ERRORS, task);
    }

    @Override
    public void onTaskTimedOut(TaskModel task) {
        publishTaskEvent(TaskModel.Status.TIMED_OUT, task);
    }

    @Override
    public void onTaskSkipped(TaskModel task) {
        publishTaskEvent(TaskModel.Status.SKIPPED, task);
    }

    private void publishTaskEvent(TaskModel.Status eventType, TaskModel task) {
        ConductorEvent message = eventFactory.buildMessage(eventType, task);
        publisher.publish(
                properties.getDefaultTopic(), task.getTaskId(), eventType.name(), message);
    }
}
