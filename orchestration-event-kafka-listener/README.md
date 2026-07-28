# Orchestration Event Kafka Listener (orchestration-kafka)

Intercepts workflow lifecycle events, packs them into the CITI GRR Orchestration `ConductorEvent`
schema, and publishes them to Kafka — independently of the existing `kafka` listener type in
`conductor-workflow-event-listener` (which publishes a different, simpler JSON shape) and of the
`workflow_publisher` (webhook) listener in that same module.

## Published Artifacts

Group: `com.netflix.conductor`

| Published Artifact | Description |
| ----------- | ----------- |
| conductor-orchestration-event-kafka-listener | Kafka publisher for workflow lifecycle events, using the CITI GRR `ConductorEvent` schema |

## Configuration

```properties
conductor.workflow-status-listener.type=orchestration-kafka

# Kafka producer properties (any org.apache.kafka.clients.producer.ProducerConfig key)
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

## Message key and payload

Each Kafka record is keyed by the workflow ID, with a JSON value shaped like:

```json
{
  "namespace": "com.citi.grr.orchestration.service.async",
  "correlationID": "...",
  "eventType": "COMPLETED",
  "eventID": "COMPLETED",
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
configurable per-deployment.

`ConductorEventFactory` builds this message from a `WorkflowModel` with no I/O involved, so it's
unit tested independently of Kafka (`ConductorEventFactoryTest`). `OrchestrationEventKafkaPublisher`
owns only producer lifecycle and delivery, and accepts an injected `Producer<String, String>` so
tests can substitute `org.apache.kafka.clients.producer.MockProducer` instead of a real broker
(`OrchestrationEventKafkaPublisherTest`).

## Running alongside other listeners

`conductor.workflow-status-listener.type` selects a single listener bean. To run
`orchestration-kafka` alongside e.g. archiving or the internal queue publisher, use the `composite`
listener type from `conductor-workflow-event-listener` and add `orchestration-kafka` to
`conductor.workflow-status-listener.composite.types` (requires registering `orchestration-kafka` as
a case in that module's `WorkflowStatusListenerFactory`).
