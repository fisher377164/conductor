# Orchestration Event Kafka Listener (orchestration-kafka)

Intercepts workflow lifecycle events and task status changes, packs them into the CITI GRR
Orchestration `ConductorEvent` schema, and publishes them to Kafka — independently of the existing
`kafka` listener type in `conductor-workflow-event-listener` (which publishes a different, simpler
JSON shape) and of the `workflow_publisher` (webhook) listener in that same module.

Three classes split the work by responsibility:

- `OrchestrationWorkflowStatusListener implements WorkflowStatusListener` — workflow lifecycle
  events only.
- `OrchestrationTaskStatusListener implements TaskStatusListener` — task status changes only.
- `OrchestrationEventKafkaPublisher` — shared delivery: serializes a `ConductorEvent` and sends it,
  logging success/failure. It implements neither listener interface itself; both listener classes
  hold one as a collaborator so the send/log/error-handling logic isn't duplicated between them.

Each listener is registered as its own Spring bean (see `OrchestrationEventKafkaPublisherConfiguration`),
gated by its own property, so either can be turned on independently — including both at once, since
neither class implements the other's interface. Both publish to the same topic (see `default-topic`
below) — there is no separate task topic or task-specific subscription filter.

## Published Artifacts

Group: `com.netflix.conductor`

| Published Artifact | Description |
| ----------- | ----------- |
| conductor-orchestration-event-kafka-listener | Kafka publisher for workflow lifecycle events, using the CITI GRR `ConductorEvent` schema |

## Configuration

```properties
conductor.workflow-status-listener.type=orchestration-kafka

# Optional: any org.apache.kafka.clients.producer.ProducerConfig key, layered on top of the
# shared KafkaProducerManager base config (see "Producer configuration" below) for anything this
# listener needs to override, e.g.:
conductor.workflow-status-listener.orchestration-kafka.producer[bootstrap.servers]=localhost:9092

# Optional: topic used when no per-event override applies (default: orchestration-workflow-events)
conductor.workflow-status-listener.orchestration-kafka.default-topic=orchestration-workflow-events

# Optional: route specific event types to their own topic
conductor.workflow-status-listener.orchestration-kafka.event-topics.COMPLETED=orchestration-workflow-completed
conductor.workflow-status-listener.orchestration-kafka.event-topics.TERMINATED=orchestration-workflow-terminated

# Optional: which lifecycle events to publish (default: all of them)
conductor.workflow-status-listener.orchestration-kafka.subscribed-events=STARTED,COMPLETED,TERMINATED
```

Supported `subscribed-events`/`event-topics` keys match
[`WorkflowStatusListener.WorkflowEventType`](../core/src/main/java/com/netflix/conductor/core/listener/WorkflowStatusListener.java):
`STARTED`, `RERAN`, `RETRIED`, `PAUSED`, `RESUMED`, `RESTARTED`, `COMPLETED`, `TERMINATED`, `FINALIZED`.

To also publish task status changes:

```properties
conductor.task-status-listener.type=orchestration-kafka
```

Task events publish unconditionally (all statuses, no per-status subscription filter) to the same
`conductor.workflow-status-listener.orchestration-kafka.default-topic` as workflow events — there
is no separate task topic or task-specific `event-topics` override. The `producer`/`default-topic`
keys stay under the `workflow-status-listener.orchestration-kafka` prefix regardless of which
listener role is active, since both share one `OrchestrationEventKafkaPublisherProperties` bean.

## Message key and payload

### Workflow events

Each Kafka record is keyed by the workflow ID, with a JSON value shaped like:

```json
{
  "namespace": "com.citi.grr.orchestration.service.async",
  "entityType": "WORKFLOW",
  "correlationID": "...",
  "eventType": "COMPLETED",
  "eventID": "3c9c1fd2-...",
  "executionId": "3c9c1fd2-...",
  "eventTimeStamp": "2026-07-28T12:00:00.000Z",
  "status": "COMPLETED",
  "statusMessage": null,
  "messageVersion": "1.0",
  "messagePayload": [
    { "name": "workflowName", "value": "my_workflow" },
    { "name": "version", "value": "1" }
  ]
}
```

`namespace` is a fixed value the downstream CITI GRR Orchestration layer filters on; it is not
configurable per-deployment. `entityType` (`WORKFLOW` or `TASK`) is what lets a consumer reading the
shared topic tell the two event kinds apart without inspecting `messagePayload`. `eventType` and
`status` both carry the same lifecycle value (e.g. `COMPLETED`); `eventID` and `executionId` both
carry the workflow's own ID, read from `WorkflowSummary#getWorkflowId()` — never generated locally.

### Task events

Each Kafka record is keyed by the task ID, with a JSON value in the same shape — `eventID` and
`executionId` both carry `TaskSummary#getTaskId()` here rather than the workflow ID; the owning
workflow's ID is carried in `messagePayload` (`workflowId`) for correlation:

```json
{
  "namespace": "com.citi.grr.orchestration.service.async",
  "entityType": "TASK",
  "correlationID": "...",
  "eventType": "COMPLETED",
  "eventID": "8f3a2e10-...",
  "executionId": "8f3a2e10-...",
  "eventTimeStamp": "2026-07-28T12:00:00.000Z",
  "status": "COMPLETED",
  "statusMessage": null,
  "messageVersion": "1.0",
  "messagePayload": [
    { "name": "workflowId", "value": "3c9c1fd2-..." },
    { "name": "taskDefName", "value": "my_task" },
    { "name": "taskType", "value": "SIMPLE" }
  ]
}
```

`ConductorEventFactory` builds both message shapes — from a `WorkflowModel` or a `TaskModel` — with
no I/O involved, so it's unit tested independently of Kafka (`ConductorEventFactoryTest`).
`OrchestrationEventKafkaPublisher` owns only delivery, not producer lifecycle, and accepts an
injected `Producer<String, String>` so tests can substitute
`org.apache.kafka.clients.producer.MockProducer` instead of a real broker
(`OrchestrationEventKafkaPublisherTest`, `OrchestrationWorkflowStatusListenerTest`,
`OrchestrationTaskStatusListenerTest`).

## Producer configuration

The Kafka producer itself is obtained from `KafkaProducerManager` (`conductor-kafka`, the same
manager the `kafka-publish` task uses), via
`KafkaProducerManager.getProducerForOverrides(Map<String, Object>)`. This means broker
connectivity/security settings (bootstrap servers, SSL, SASL, truststore, etc.) configured once,
globally, for `KafkaProducerManager` are shared by this listener automatically — there is no need
to duplicate them under `conductor.workflow-status-listener.orchestration-kafka.producer`. That
`producer` map is only for overriding specific keys for this listener; anything not set there
falls through to the manager's shared base config. `KafkaProducerManager` also owns the producer's
lifecycle (a shared, size/time-bounded cache keyed by resolved properties), so this listener does
not close the producer itself.

## Running alongside other listeners

`conductor.workflow-status-listener.type` and `conductor.task-status-listener.type` each select a
single listener bean of their respective type — one for workflows, one for tasks — so
`orchestration-kafka` can be the workflow listener, the task listener, or both, independently of
each other.

To run `orchestration-kafka` alongside another *workflow* listener, e.g. archiving or the internal
queue publisher, use the `composite` listener type from `conductor-workflow-event-listener` and add
`orchestration-kafka` to `conductor.workflow-status-listener.composite.types` (requires registering
`orchestration-kafka` as a case in that module's `WorkflowStatusListenerFactory`). There is no
equivalent `composite` mechanism for `conductor.task-status-listener.type`, so `orchestration-kafka`
cannot currently run alongside another *task* listener (e.g. `task-status-listener`'s
`task_publisher`) — only one task listener bean can be active at a time.
