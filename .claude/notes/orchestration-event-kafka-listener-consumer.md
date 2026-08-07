# Orchestration event Kafka listener (consumer side) — design notes

Companion to the existing publish-only `orchestration-event-kafka-listener` module (which,
despite the module name, only *publishes* `ConductorEvent` to Kafka via
`OrchestrationEventKafkaPublisher`). This adds the read side: a listener that consumes
`ConductorEvent` messages back off a Kafka topic and hands them to a pluggable processor.

Code lives under `.claude/notes/orchestration-event-kafka-listener-consumer/` (package-correct
path, not wired into the Gradle build) since it's a design draft, not a merge-ready change.

## Files

- `ConductorEventProcessor` — the extension point. One method, `process(ConductorEvent)`. **No
  real implementation yet** — this is intentionally left for future work to plug in as a Spring
  bean (persistence, forwarding to a downstream system, triggering workflow actions, etc.).
- `LoggingConductorEventProcessor` — placeholder default (`@ConditionalOnMissingBean`) so the
  listener has somewhere to send events before a real processor exists. Just logs.
- `KafkaSslProperties` — record holding the consumer's SSL settings (protocol, truststore,
  keystore, hostname verification). Records keep component names in class metadata
  unconditionally, so Spring Boot property binding works without the `-parameters` compiler flag.
- `KafkaConsumerProperties` — record for the consumer-specific Kafka settings (bootstrap servers,
  offset reset, auto-commit, poll sizing, timeouts), nesting `KafkaSslProperties`, plus an
  `additionalProperties` map as an escape hatch for one-off `ConsumerConfig` keys. Kept as its own
  type instead of a raw `Map<String, Object>` (unlike
  `OrchestrationEventKafkaPublisherProperties#getProducer()`) per the ask that custom Kafka
  properties get a dedicated Spring properties class/record.
- `OrchestrationEventKafkaListenerProperties` — top-level `@ConfigurationProperties` under
  `conductor.event-listener.orchestration-kafka` (topic, group id, poll timeout, concurrency, plus
  the nested `consumer` block). Uses Lombok `@Getter`/`@Setter` — mutable-class binding like the
  existing publisher properties, just without the hand-written accessors.
- `ConductorEventKafkaListener` — the actual consumer. Built directly on `kafka-clients`'
  `Consumer`/`KafkaConsumer` (this module has no `spring-kafka` dependency, and the existing
  publisher builds its `Producer` the same direct way). Implements Spring's `SmartLifecycle`;
  `concurrency` worker threads each own one `Consumer` in the same group, so Kafka load-balances
  partitions across them. Commits offsets synchronously per poll batch, only once every record in
  the batch has been processed without throwing — at-least-once delivery, so
  `ConductorEventProcessor` implementations must be idempotent/duplicate-tolerant.
- `OrchestrationEventKafkaListenerConfiguration` — `@Configuration`, gated behind
  `conductor.event-listener.orchestration-kafka.enabled=true` so it doesn't affect deployments
  that only run the existing publish side.

## Why manual JSON decoding instead of `objectMapper.readValue(json, ConductorEvent.class)`

`ConductorEvent` (existing production class, unmodified here) only has an all-args constructor —
no setters, no `@JsonCreator`/`@JsonProperty`. Jackson can't build it without either those
annotations or the `jackson-module-parameter-names` module plus a `-parameters` compile flag,
neither of which the module currently has. `ConductorEventKafkaListener.decode(String)` instead
reads the payload into a `JsonNode` tree and constructs the record's fields by hand — works today
with zero changes to `ConductorEvent`. If `@JsonCreator`/`@JsonProperty` are ever added there
(low-risk, additive), `decode` can be simplified to a direct `readValue` call.

## Example configuration (SSL enabled)

```yaml
conductor:
  event-listener:
    orchestration-kafka:
      enabled: true
      topic: orchestration-workflow-events
      group-id: my-service-orchestration-listener
      poll-timeout: 1s
      concurrency: 3
      consumer:
        bootstrap-servers: kafka.internal:9093
        auto-offset-reset: earliest
        enable-auto-commit: false
        max-poll-records: 500
        session-timeout: 45s
        request-timeout: 30s
        ssl:
          enabled: true
          protocol: SSL
          trust-store-location: /etc/kafka/ssl/truststore.jks
          trust-store-password: ${KAFKA_TRUSTSTORE_PASSWORD}
          key-store-location: /etc/kafka/ssl/keystore.jks
          key-store-password: ${KAFKA_KEYSTORE_PASSWORD}
          key-password: ${KAFKA_KEY_PASSWORD}
        additional-properties:
          "[fetch.min.bytes]": "1024"
```

## Open items before this could actually ship

- Real `ConductorEventProcessor` implementation — the whole reason this listener exists.
- Decide whether SSL/bootstrap defaults should fall back to the shared `KafkaProducerManager`
  system-property style (`bootstrap.servers`, `ssl.truststore.location`, etc.) the way the
  publisher's producer overrides do, instead of requiring the consumer block to repeat them.
  Left out here to keep `KafkaConsumerProperties` self-contained and independently testable.
- Metrics/health signal for consumer lag and processing failures.
- Dead-letter handling for records that repeatedly fail `ConductorEventProcessor.process`.
