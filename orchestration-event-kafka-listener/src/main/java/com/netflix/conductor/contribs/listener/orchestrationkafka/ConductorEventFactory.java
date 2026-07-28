package com.netflix.conductor.contribs.listener.orchestrationkafka;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.common.run.WorkflowSummaryExtended;
import com.netflix.conductor.core.listener.WorkflowStatusListener.WorkflowEventType;
import com.netflix.conductor.model.WorkflowModel;

/**
 * Packs a workflow lifecycle event into the CITI GRR Orchestration {@link ConductorEvent} schema.
 * Pure transformation, no I/O, so it can be unit tested without a Kafka broker.
 */
public class ConductorEventFactory {

    /** Fixed value the Orchestration layer filters on; not configurable per-deployment. */
    private static final String NAMESPACE = "com.citi.grr.orchestration.service.async";

    private static final String MESSAGE_VERSION = "1.0";

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    /**
     * Builds the outbound message for the CITI GRR Orchestration schema. {@code eventType},
     * {@code eventID} and {@code status} all carry the same {@link WorkflowEventType} lifecycle
     * value; anything else useful for debugging goes into {@code messagePayload}, one entry per
     * {@link PayloadField}.
     */
    public ConductorEvent buildMessage(WorkflowEventType eventType, WorkflowModel workflow) {
        WorkflowSummary workflowSummary = new WorkflowSummaryExtended(workflow.toWorkflow());
        String eventTypeName = eventType.name();
        PayloadSource source = new PayloadSource(workflow, workflowSummary);

        List<MessagePayloadEntry> payload = new ArrayList<>();
        for (PayloadField field : PayloadField.values()) {
            addIfPresent(payload, field.fieldName(), field.extractValue(source));
        }

        return new ConductorEvent(
                NAMESPACE,
                workflow.getCorrelationId(),
                eventTypeName,
                eventTypeName,
                workflow.getWorkflowId(),
                TIMESTAMP_FORMATTER.format(Instant.now()),
                eventTypeName,
                workflow.getReasonForIncompletion(),
                MESSAGE_VERSION,
                payload);
    }

    private static void addIfPresent(List<MessagePayloadEntry> payload, String name, String value) {
        if (value != null && !value.isEmpty()) {
            payload.add(new MessagePayloadEntry(name, value));
        }
    }

    /** Bundles the raw workflow with its derived summary so {@link PayloadField} extractors can read from either. */
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

    /** Name/value-extractor pairs that make up {@link ConductorEvent#getMessagePayload()}. */
    private enum PayloadField {
        WORKFLOW_NAME("workflowName", wf -> wf.getWorkflowName()),
        VERSION("version", wf -> wf.getVersion()),
        START_TIME("startTime", wf -> wf.getStartTime()),
        END_TIME("endTime", wf -> wf.getEndTime()),
        UPDATE_TIME("updateTime", wf -> wf.getUpdateTime()),
        EXECUTION_TIME("executionTime", wf -> wf.getExecutionTime()),
        PRIORITY("priority", wf -> wf.getPriority()),
        CREATED_BY("createdBy", wf -> wf.getCreatedBy()),
        IDEMPOTENCY_KEY("idempotencyKey", wf -> wf.getIdempotencyKey()),
        FAILED_TASK_NAMES("failedTaskNames", wf -> wf.getFailedTaskNames()),
        INPUT("input", wf -> wf.getInput()),
        OUTPUT("output", wf -> wf.getOutput());

        private final String fieldName;
        private final Function<PayloadSource, String> valueExtractor;

        PayloadField(String fieldName, Function<PayloadSource, String> valueExtractor) {
            this.fieldName = fieldName;
            this.valueExtractor = valueExtractor;
        }

        String fieldName() {
            return fieldName;
        }

        String extractValue(PayloadSource source) {
            return valueExtractor.apply(source);
        }
    }
}
