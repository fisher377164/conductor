package com.example.kafka.event;

import java.util.List;

public record ConductorEvent(
        String namespace,
        String correlationID,
        String eventType,
        String eventID,
        String executionId,
        String eventTimeStamp,
        String status,
        String statusMessage,
        String messageVersion,
        List<MessagePayloadEntry> messagePayload
) {
}