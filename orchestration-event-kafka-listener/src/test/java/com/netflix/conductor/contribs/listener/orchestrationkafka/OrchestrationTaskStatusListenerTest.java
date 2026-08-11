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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.netflix.conductor.model.TaskModel;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OrchestrationTaskStatusListenerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private MockProducer<String, String> mockProducer;
    private OrchestrationTaskStatusListener listener;

    @BeforeEach
    void setUp() {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        OrchestrationEventKafkaPublisherProperties properties =
                new OrchestrationEventKafkaPublisherProperties();
        properties.setDefaultTopic("orchestration-workflow-events");
        listener =
                new OrchestrationTaskStatusListener(
                        properties,
                        new OrchestrationEventKafkaPublisher(objectMapper, mockProducer));
    }

    private TaskModel task(String id, String workflowId, String taskDefName) {
        TaskModel task = new TaskModel();
        task.setTaskId(id);
        task.setWorkflowInstanceId(workflowId);
        task.setTaskDefName(taskDefName);
        task.setStatus(TaskModel.Status.COMPLETED);
        return task;
    }

    @Test
    void publishesTaskCompletedEventToTheSharedTopic() throws Exception {
        listener.onTaskCompleted(task("task-1", "wf-1", "test-task"));

        List<ProducerRecord<String, String>> history = mockProducer.history();
        assertEquals(1, history.size());
        ProducerRecord<String, String> record = history.get(0);
        assertEquals("orchestration-workflow-events", record.topic());
        assertEquals("task-1", record.key());

        JsonNode json = objectMapper.readTree(record.value());
        assertEquals("com.citi.grr.orchestration.service.async", json.get("namespace").asText());
        assertEquals("COMPLETED", json.get("eventType").asText());
        assertEquals("task-1", json.get("executionId").asText());
    }

    @Test
    void publishesTaskScheduledEvent() throws Exception {
        listener.onTaskScheduled(task("task-2", "wf-1", "test-task"));

        JsonNode json = objectMapper.readTree(mockProducer.history().get(0).value());
        assertEquals("SCHEDULED", json.get("eventType").asText());
    }
}
