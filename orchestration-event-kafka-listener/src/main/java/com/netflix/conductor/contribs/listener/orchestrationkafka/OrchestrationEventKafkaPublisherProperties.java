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
     * Maps {@link #producer} against {@link ProducerConfig} keys, filling in sane defaults for
     * anything the caller didn't set.
     */
    public Map<String, Object> toProducerConfig() {
        Map<String, Object> config = new HashMap<>();
        for (String key : ProducerConfig.configNames()) {
            if (producer.containsKey(key)) {
                config.put(key, producer.get(key));
            }
        }

        setDefaultIfBlank(config, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:29092");
        setDefaultIfBlank(
                config,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        setDefaultIfBlank(
                config,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        setDefaultIfBlank(config, ProducerConfig.CLIENT_ID_CONFIG, "orchestration-event-kafka-publisher");

        return config;
    }

    private static void setDefaultIfBlank(
            Map<String, Object> config, String key, String defaultValue) {
        Object value = config.get(key);
        if (value == null || (value instanceof String stringValue && stringValue.isBlank())) {
            config.put(key, defaultValue);
        }
    }
}
