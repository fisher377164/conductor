# orchestration-event-kafka-listener — CITI GRR schema validation

Validated `orchestration-event-kafka-listener` against the required CITI GRR
Orchestration async Kafka schema. **Does not conform** — 3 of 10 fields carry
the wrong values.

## Required schema

```json
{
  "namespace": "com.citi.grr.orchestration.service.async",
  "correlationID": "String",
  "eventType": "String",           // one of: REPORT_GENERATION, FORMSPEC_EXE, EDITCHECK_EXE, ANALYTICAL_EXE, TOPSHEET_EXE, etc.
  "eventID": "String",             // used for sequencing events
  "executionId": "String",
  "eventTimeStamp": "String",      // UTC, e.g. 2019-11-26T05:00:00.000Z
  "status": "String",              // one of: SUBMITTED, REPORT_GENERATED, REPORT_SUBMITTED, FAILED, COMPLETED
  "statusMessage": "String",       // required for FAILED status
  "messageVersion": "String",
  "messagePayload": [ { "name": "someKey", "value": "someValue" } ]
}
```

## Matches

| Field | Code | Verdict |
|---|---|---|
| `namespace` | hardcoded `"com.citi.grr.orchestration.service.async"` (`ConductorEventFactory.java:23`) | OK |
| `correlationID` | `workflow.getCorrelationId()` | OK |
| `executionId` | `workflow.getWorkflowId()` | OK |
| `eventTimeStamp` | `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'` in UTC | OK, matches example format |
| `messageVersion` | hardcoded `"1.0"` | OK |
| `messagePayload` | `List<MessagePayloadEntry>` with `name`/`value` getters | OK, structurally correct |

## Does not match

| Field | Schema requires | Code actually sends | Problem |
|---|---|---|---|
| `eventType` | one of `REPORT_GENERATION`, `FORMSPEC_EXE`, `EDITCHECK_EXE`, `ANALYTICAL_EXE`, `TOPSHEET_EXE` | Conductor's `WorkflowEventType.name()`: `STARTED`, `RERAN`, `RETRIED`, `PAUSED`, `RESUMED`, `RESTARTED`, `COMPLETED`, `TERMINATED`, `FINALIZED` | Wrong domain entirely — workflow lifecycle transitions, not business/report process types. No value in the code will ever match the required enum. |
| `eventID` | distinct value usable for sequencing | Same string as `eventType` (`ConductorEventFactory.java:49-50`; confirmed by `ConductorEventFactoryTest`: `assertEquals("COMPLETED", event.getEventID())`) | Literal duplicate of `eventType` — no monotonic counter, timestamp-derived value, or ordinal exists anywhere. |
| `status` | one of `SUBMITTED`, `REPORT_GENERATED`, `REPORT_SUBMITTED`, `FAILED`, `COMPLETED` | Same `WorkflowEventType.name()` again | Only `COMPLETED` overlaps. `STARTED`, `PAUSED`, `RESUMED`, `TERMINATED`, etc. are outside the allowed set, and nothing maps to `FAILED`. |
| `statusMessage` | required when `status == FAILED` | `workflow.getReasonForIncompletion()` | Since `status` never actually equals `"FAILED"`, this required-when-FAILED coupling can't be honored even when it should be. |

## Root cause

`ConductorEventFactory.buildMessage` (`ConductorEventFactory.java:36-57`) reuses
one `WorkflowEventType eventType` parameter for all three of `eventType`,
`eventID`, and `status`, with no translation layer to the CITI GRR vocabulary.
The README (`README.md:47-51`) documents this as intentional current behavior,
not a bug — so this is a real design gap against the schema, not an oversight
in otherwise-correct code.

## Fix direction (not yet implemented)

- `WorkflowEventType → eventType`: needs to come from workflow/task metadata
  (business process type), not the lifecycle event.
- `WorkflowEventType → status`: mapping table, e.g. `STARTED`→`SUBMITTED`,
  `COMPLETED`→`COMPLETED`, `TERMINATED`/failure paths→`FAILED`.
- `eventID`: needs a real value independent of `eventType` (UUID or
  incrementing sequence).
