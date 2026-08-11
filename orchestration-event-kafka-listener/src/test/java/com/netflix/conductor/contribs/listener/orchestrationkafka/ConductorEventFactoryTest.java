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

import org.junit.jupiter.api.Test;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.listener.WorkflowStatusListener.WorkflowEventType;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConductorEventFactoryTest {

    private final ConductorEventFactory factory = new ConductorEventFactory();

    @Test
    void buildsMessageFromWorkflow() {
        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowId("wf-1");
        workflow.setCorrelationId("corr-1");
        workflow.setWorkflowDefinition(new WorkflowDef());
        workflow.getWorkflowDefinition().setName("test-workflow");

        ConductorEvent event = factory.buildMessage(WorkflowEventType.COMPLETED, workflow);

        assertEquals("com.citi.grr.orchestration.service.async", event.getNamespace());
        assertEquals(ConductorEvent.EntityType.WORKFLOW, event.getEntityType());
        assertEquals("corr-1", event.getCorrelationID());
        assertEquals("COMPLETED", event.getEventType());
        assertEquals("wf-1", event.getEventID());
        assertEquals("COMPLETED", event.getStatus());
        assertEquals("wf-1", event.getExecutionId());
        assertTrue(
                event.getMessagePayload().stream()
                        .anyMatch(
                                entry ->
                                        "workflowName".equals(entry.getName())
                                                && "test-workflow".equals(entry.getValue())));
    }

    @Test
    void buildsMessageFromTask() {
        TaskModel task = new TaskModel();
        task.setTaskId("task-1");
        task.setCorrelationId("corr-1");
        task.setWorkflowInstanceId("wf-1");
        task.setTaskDefName("test-task");
        task.setTaskType("SIMPLE");
        task.setReferenceTaskName("test-task-ref");
        task.setStatus(TaskModel.Status.COMPLETED);

        ConductorEvent event = factory.buildMessage(TaskModel.Status.COMPLETED, task);

        assertEquals("com.citi.grr.orchestration.service.async", event.getNamespace());
        assertEquals(ConductorEvent.EntityType.TASK, event.getEntityType());
        assertEquals("corr-1", event.getCorrelationID());
        assertEquals("COMPLETED", event.getEventType());
        assertEquals("task-1", event.getEventID());
        assertEquals("COMPLETED", event.getStatus());
        assertEquals("task-1", event.getExecutionId());
        assertTrue(
                event.getMessagePayload().stream()
                        .anyMatch(
                                entry ->
                                        "taskDefName".equals(entry.getName())
                                                && "test-task".equals(entry.getValue())));
        assertTrue(
                event.getMessagePayload().stream()
                        .anyMatch(
                                entry ->
                                        "workflowId".equals(entry.getName())
                                                && "wf-1".equals(entry.getValue())));
    }

    @Test
    void omitsExecutionTimeUntilWorkflowEnds() {
        WorkflowModel workflow = new WorkflowModel();
        workflow.setWorkflowId("wf-2");
        workflow.setWorkflowDefinition(new WorkflowDef());
        workflow.getWorkflowDefinition().setName("test-workflow");

        ConductorEvent event = factory.buildMessage(WorkflowEventType.STARTED, workflow);

        assertTrue(
                event.getMessagePayload().stream()
                        .noneMatch(entry -> "executionTime".equals(entry.getName())));
    }
}
