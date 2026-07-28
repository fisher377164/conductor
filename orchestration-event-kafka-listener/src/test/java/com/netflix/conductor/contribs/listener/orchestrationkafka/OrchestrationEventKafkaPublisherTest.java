package com.netflix.conductor.contribs.listener.orchestrationkafka;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.listener.WorkflowStatusListener.WorkflowEventType;
import com.netflix.conductor.model.WorkflowModel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrchestrationEventKafkaPublisherTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private MockProducer<String, String> mockProducer;
    private OrchestrationEventKafkaPublisher listener;

    @BeforeEach
    void setUp() {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        OrchestrationEventKafkaPublisherProperties properties = new OrchestrationEventKafkaPublisherProperties();
        properties.setDefaultTopic("orchestration-workflow-events");
        listener = new OrchestrationEventKafkaPublisher(properties, objectMapper, mockProducer);
    }

    private WorkflowModel workflow(String id, String name) {
        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowId(id);
        workflow.setWorkflowDefinition(new WorkflowDef());
        workflow.getWorkflowDefinition().setName(name);
        return workflow;
    }

    @Test
    void publishesWorkflowCompletedEvent() throws Exception {
        listener.onWorkflowCompleted(workflow("wf-1", "test-workflow"));

        List<ProducerRecord<String, String>> history = mockProducer.history();
        assertEquals(1, history.size());
        ProducerRecord<String, String> record = history.get(0);
        assertEquals("orchestration-workflow-events", record.topic());
        assertEquals("wf-1", record.key());

        JsonNode json = objectMapper.readTree(record.value());
        assertEquals("com.citi.grr.orchestration.service.async", json.get("namespace").asText());
        assertEquals("COMPLETED", json.get("eventType").asText());
        assertEquals("wf-1", json.get("executionId").asText());
    }

    @Test
    void publishesWorkflowTerminatedEvent() throws Exception {
        listener.onWorkflowTerminated(workflow("wf-2", "test-workflow"));

        List<ProducerRecord<String, String>> history = mockProducer.history();
        assertEquals(1, history.size());
        JsonNode json = objectMapper.readTree(history.get(0).value());
        assertEquals("TERMINATED", json.get("eventType").asText());
        assertEquals("wf-2", json.get("executionId").asText());
    }

    @Test
    void routesEventToPerEventTopicOverride() throws Exception {
        OrchestrationEventKafkaPublisherProperties properties = new OrchestrationEventKafkaPublisherProperties();
        properties.setDefaultTopic("orchestration-workflow-events");
        properties.setEventTopics(Map.of(WorkflowEventType.COMPLETED, "orchestration-workflow-completed"));
        OrchestrationEventKafkaPublisher overriddenListener =
                new OrchestrationEventKafkaPublisher(properties, objectMapper, mockProducer);

        overriddenListener.onWorkflowCompleted(workflow("wf-3", "test-workflow"));

        assertEquals("orchestration-workflow-completed", mockProducer.history().get(0).topic());
    }

    @Test
    void skipsEventsNotInSubscribedList() {
        OrchestrationEventKafkaPublisherProperties properties = new OrchestrationEventKafkaPublisherProperties();
        properties.setSubscribedEvents(List.of(WorkflowEventType.COMPLETED));
        OrchestrationEventKafkaPublisher completedOnlyListener =
                new OrchestrationEventKafkaPublisher(properties, objectMapper, mockProducer);

        completedOnlyListener.onWorkflowStarted(workflow("wf-4", "test-workflow"));

        assertTrue(
                mockProducer.history().isEmpty(),
                "STARTED should not be published when only COMPLETED is subscribed");
    }
}
