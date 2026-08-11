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

import java.util.List;

/** Outbound event message matching the CITI GRR Orchestration async Kafka schema. */
public class ConductorEvent {

    /**
     * Distinguishes a workflow lifecycle event from a task status event. Both share one Kafka topic
     * (see {@link OrchestrationEventKafkaPublisherProperties#getDefaultTopic()}), so consumers need
     * this to tell the two apart without inspecting {@code messagePayload}.
     */
    public enum EntityType {
        WORKFLOW,
        TASK
    }

    private final String namespace;
    private final EntityType entityType;
    private final String correlationID;
    private final String eventType;
    private final String eventID;
    private final String executionId;
    private final String eventTimeStamp;
    private final String status;
    private final String statusMessage;
    private final String messageVersion;
    private final List<MessagePayloadEntry> messagePayload;

    public ConductorEvent(
            String namespace,
            EntityType entityType,
            String correlationID,
            String eventType,
            String eventID,
            String executionId,
            String eventTimeStamp,
            String status,
            String statusMessage,
            String messageVersion,
            List<MessagePayloadEntry> messagePayload) {
        this.namespace = namespace;
        this.entityType = entityType;
        this.correlationID = correlationID;
        this.eventType = eventType;
        this.eventID = eventID;
        this.executionId = executionId;
        this.eventTimeStamp = eventTimeStamp;
        this.status = status;
        this.statusMessage = statusMessage;
        this.messageVersion = messageVersion;
        this.messagePayload = messagePayload;
    }

    public String getNamespace() {
        return namespace;
    }

    public EntityType getEntityType() {
        return entityType;
    }

    public String getCorrelationID() {
        return correlationID;
    }

    public String getEventType() {
        return eventType;
    }

    public String getEventID() {
        return eventID;
    }

    public String getExecutionId() {
        return executionId;
    }

    public String getEventTimeStamp() {
        return eventTimeStamp;
    }

    public String getStatus() {
        return status;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public String getMessageVersion() {
        return messageVersion;
    }

    public List<MessagePayloadEntry> getMessagePayload() {
        return messagePayload;
    }
}
