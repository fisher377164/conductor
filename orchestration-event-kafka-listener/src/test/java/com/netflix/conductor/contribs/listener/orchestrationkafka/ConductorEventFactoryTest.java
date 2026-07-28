package com.netflix.conductor.contribs.listener.orchestrationkafka;

import org.junit.jupiter.api.Test;

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.listener.WorkflowStatusListener.WorkflowEventType;
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
        assertEquals("corr-1", event.getCorrelationID());
        assertEquals("COMPLETED", event.getEventType());
        assertEquals("COMPLETED", event.getEventID());
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
