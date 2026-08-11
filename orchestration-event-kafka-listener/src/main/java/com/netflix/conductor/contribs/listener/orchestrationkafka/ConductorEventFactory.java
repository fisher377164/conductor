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

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.common.run.WorkflowSummaryExtended;
import com.netflix.conductor.contribs.listener.orchestrationkafka.ConductorEvent.EntityType;
import com.netflix.conductor.core.listener.WorkflowStatusListener.WorkflowEventType;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

/**
 * Packs a workflow lifecycle event or a task status event into the CITI GRR Orchestration {@link
 * ConductorEvent} schema. Pure transformation, no I/O, so it can be unit tested without a Kafka
 * broker.
 */
public class ConductorEventFactory {

    /** Fixed value the Orchestration layer filters on; not configurable per-deployment. */
    private static final String NAMESPACE = "com.citi.grr.orchestration.service.async";

    private static final String MESSAGE_VERSION = "1.0";

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    /**
     * Builds the outbound message for a workflow lifecycle event. {@code eventType} and {@code
     * status} both carry the same {@link WorkflowEventType} lifecycle value; {@code eventID} and
     * {@code executionId} both carry {@link WorkflowSummary#getWorkflowId()}. Anything else useful
     * for debugging goes into {@code messagePayload}, one entry per {@link PayloadField}.
     */
    public ConductorEvent buildMessage(WorkflowEventType eventType, WorkflowModel workflow) {
        WorkflowSummary workflowSummary = new WorkflowSummaryExtended(workflow.toWorkflow());
        PayloadSource source = new PayloadSource(workflow, workflowSummary);

        return buildEvent(
                EntityType.WORKFLOW,
                eventType.name(),
                workflow.getCorrelationId(),
                workflowSummary.getWorkflowId(),
                workflow.getReasonForIncompletion(),
                buildPayload(source, PayloadField.values()));
    }

    /**
     * Builds the outbound message for a task status event, using the same CITI GRR Orchestration
     * schema as {@link #buildMessage(WorkflowEventType, WorkflowModel)}. The task is the execution
     * unit here, so {@code eventID}/{@code executionId} carry {@link TaskSummary#getTaskId()}
     * rather than the workflow ID; the owning workflow's ID is included in {@code messagePayload}
     * for correlation.
     */
    public ConductorEvent buildMessage(TaskModel.Status eventType, TaskModel task) {
        TaskSummary taskSummary = new TaskSummary(task.toTask());
        TaskPayloadSource source = new TaskPayloadSource(task, taskSummary);

        return buildEvent(
                EntityType.TASK,
                eventType.name(),
                task.getCorrelationId(),
                taskSummary.getTaskId(),
                task.getReasonForIncompletion(),
                buildPayload(source, TaskPayloadField.values()));
    }

    /**
     * Assembles the envelope shared by workflow and task events: {@code eventTypeName} fills both
     * {@code eventType} and {@code status} (the same lifecycle value read two ways); {@code id}
     * (the workflow/task ID taken from the caller's {@code WorkflowSummary}/{@code TaskSummary},
     * never generated here) fills both {@code eventID} and {@code executionId}; {@code entityType}
     * is what lets a consumer of the shared topic tell a workflow event from a task event.
     */
    private static ConductorEvent buildEvent(
            EntityType entityType,
            String eventTypeName,
            String correlationId,
            String id,
            String reasonForIncompletion,
            List<MessagePayloadEntry> payload) {
        return new ConductorEvent(
                NAMESPACE,
                entityType,
                correlationId,
                eventTypeName,
                id,
                id,
                TIMESTAMP_FORMATTER.format(Instant.now()),
                eventTypeName,
                reasonForIncompletion,
                MESSAGE_VERSION,
                payload);
    }

    /** Runs {@code fields} over {@code source}, skipping any that extract to null/empty. */
    private static <T> List<MessagePayloadEntry> buildPayload(T source, FieldSpec<T>[] fields) {
        List<MessagePayloadEntry> payload = new ArrayList<>();
        for (FieldSpec<T> field : fields) {
            addIfPresent(payload, field.fieldName(), field.extractValue(source));
        }
        return payload;
    }

    private static void addIfPresent(List<MessagePayloadEntry> payload, String name, String value) {
        if (value != null && !value.isEmpty()) {
            payload.add(new MessagePayloadEntry(name, value));
        }
    }

    /**
     * Common shape for {@link PayloadField} and {@link TaskPayloadField} so {@link #buildPayload}
     * can run either.
     */
    private interface FieldSpec<T> {
        String fieldName();

        String extractValue(T source);
    }

    /**
     * Bundles the raw workflow with its derived summary so {@link PayloadField} extractors can read
     * from either.
     */
    private record PayloadSource(WorkflowModel workflow, WorkflowSummary summary) {

        String getWorkflowName() {
            return workflow.getWorkflowName();
        }

        String getVersion() {
            return String.valueOf(summary.getVersion());
        }

        String getStartTime() {
            return summary.getStartTime();
        }

        String getEndTime() {
            return summary.getEndTime();
        }

        String getUpdateTime() {
            return summary.getUpdateTime();
        }

        String getExecutionTime() {
            return summary.getEndTime() != null ? String.valueOf(summary.getExecutionTime()) : null;
        }

        String getPriority() {
            return String.valueOf(summary.getPriority());
        }

        String getCreatedBy() {
            return summary.getCreatedBy();
        }

        String getIdempotencyKey() {
            return summary.getIdempotencyKey();
        }

        String getFailedTaskNames() {
            Set<String> failedTaskNames = summary.getFailedTaskNames();
            return failedTaskNames == null || failedTaskNames.isEmpty()
                    ? null
                    : String.join(",", failedTaskNames);
        }

        String getInput() {
            return summary.getInput();
        }

        String getOutput() {
            return summary.getOutput();
        }
    }

    /**
     * Name/value-extractor pairs that make up {@link ConductorEvent#getMessagePayload()} for
     * workflow events.
     */
    private enum PayloadField implements FieldSpec<PayloadSource> {
        WORKFLOW_NAME("workflowName", PayloadSource::getWorkflowName),
        VERSION("version", PayloadSource::getVersion),
        START_TIME("startTime", PayloadSource::getStartTime),
        END_TIME("endTime", PayloadSource::getEndTime),
        UPDATE_TIME("updateTime", PayloadSource::getUpdateTime),
        EXECUTION_TIME("executionTime", PayloadSource::getExecutionTime),
        PRIORITY("priority", PayloadSource::getPriority),
        CREATED_BY("createdBy", PayloadSource::getCreatedBy),
        IDEMPOTENCY_KEY("idempotencyKey", PayloadSource::getIdempotencyKey),
        FAILED_TASK_NAMES("failedTaskNames", PayloadSource::getFailedTaskNames),
        INPUT("input", PayloadSource::getInput),
        OUTPUT("output", PayloadSource::getOutput);

        private final String fieldName;
        private final Function<PayloadSource, String> valueExtractor;

        PayloadField(String fieldName, Function<PayloadSource, String> valueExtractor) {
            this.fieldName = fieldName;
            this.valueExtractor = valueExtractor;
        }

        @Override
        public String fieldName() {
            return fieldName;
        }

        @Override
        public String extractValue(PayloadSource source) {
            return valueExtractor.apply(source);
        }
    }

    /**
     * Bundles the raw task with its derived summary so {@link TaskPayloadField} extractors can read
     * from either.
     */
    private record TaskPayloadSource(TaskModel task, TaskSummary summary) {

        String getWorkflowId() {
            return summary.getWorkflowId();
        }

        String getWorkflowType() {
            return summary.getWorkflowType();
        }

        String getTaskType() {
            return summary.getTaskType();
        }

        String getTaskDefName() {
            return summary.getTaskDefName();
        }

        String getReferenceTaskName() {
            return task.getReferenceTaskName();
        }

        String getScheduledTime() {
            return summary.getScheduledTime();
        }

        String getStartTime() {
            return summary.getStartTime();
        }

        String getEndTime() {
            return summary.getEndTime();
        }

        String getUpdateTime() {
            return summary.getUpdateTime();
        }

        String getExecutionTime() {
            return task.getEndTime() > 0 ? String.valueOf(summary.getExecutionTime()) : null;
        }

        String getQueueWaitTime() {
            return String.valueOf(summary.getQueueWaitTime());
        }

        String getWorkflowPriority() {
            return String.valueOf(summary.getWorkflowPriority());
        }

        String getRetryCount() {
            return String.valueOf(task.getRetryCount());
        }

        String getPollCount() {
            return String.valueOf(task.getPollCount());
        }

        String getWorkerId() {
            return task.getWorkerId();
        }

        String getDomain() {
            return summary.getDomain();
        }

        String getInput() {
            return summary.getInput();
        }

        String getOutput() {
            return summary.getOutput();
        }
    }

    /**
     * Name/value-extractor pairs that make up {@link ConductorEvent#getMessagePayload()} for task
     * events.
     */
    private enum TaskPayloadField implements FieldSpec<TaskPayloadSource> {
        WORKFLOW_ID("workflowId", TaskPayloadSource::getWorkflowId),
        WORKFLOW_TYPE("workflowType", TaskPayloadSource::getWorkflowType),
        TASK_TYPE("taskType", TaskPayloadSource::getTaskType),
        TASK_DEF_NAME("taskDefName", TaskPayloadSource::getTaskDefName),
        REFERENCE_TASK_NAME("referenceTaskName", TaskPayloadSource::getReferenceTaskName),
        SCHEDULED_TIME("scheduledTime", TaskPayloadSource::getScheduledTime),
        START_TIME("startTime", TaskPayloadSource::getStartTime),
        END_TIME("endTime", TaskPayloadSource::getEndTime),
        UPDATE_TIME("updateTime", TaskPayloadSource::getUpdateTime),
        EXECUTION_TIME("executionTime", TaskPayloadSource::getExecutionTime),
        QUEUE_WAIT_TIME("queueWaitTime", TaskPayloadSource::getQueueWaitTime),
        WORKFLOW_PRIORITY("workflowPriority", TaskPayloadSource::getWorkflowPriority),
        RETRY_COUNT("retryCount", TaskPayloadSource::getRetryCount),
        POLL_COUNT("pollCount", TaskPayloadSource::getPollCount),
        WORKER_ID("workerId", TaskPayloadSource::getWorkerId),
        DOMAIN("domain", TaskPayloadSource::getDomain),
        INPUT("input", TaskPayloadSource::getInput),
        OUTPUT("output", TaskPayloadSource::getOutput);

        private final String fieldName;
        private final Function<TaskPayloadSource, String> valueExtractor;

        TaskPayloadField(String fieldName, Function<TaskPayloadSource, String> valueExtractor) {
            this.fieldName = fieldName;
            this.valueExtractor = valueExtractor;
        }

        @Override
        public String fieldName() {
            return fieldName;
        }

        @Override
        public String extractValue(TaskPayloadSource source) {
            return valueExtractor.apply(source);
        }
    }
}
