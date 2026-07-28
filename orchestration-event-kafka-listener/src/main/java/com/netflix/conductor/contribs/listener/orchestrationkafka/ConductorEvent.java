package com.netflix.conductor.contribs.listener.orchestrationkafka;

import java.util.List;

/** Outbound event message matching the CITI GRR Orchestration async Kafka schema. */
public class ConductorEvent {

    private final String namespace;
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
