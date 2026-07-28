package com.netflix.conductor.contribs.listener.orchestrationkafka;

/** A single name/value entry in {@link ConductorEvent#getMessagePayload()}. */
public class MessagePayloadEntry {

    private final String name;
    private final String value;

    public MessagePayloadEntry(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }
}
