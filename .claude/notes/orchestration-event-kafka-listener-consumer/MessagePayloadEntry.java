package com.example.kafka.event;

public record MessagePayloadEntry(
        String name,
        String value
) {
}