# Sharing KafkaProducerManager with orchestration-event-kafka-listener

## What the uncommitted diff does

Adds two members to `KafkaProducerManager` (in the `kafka` module):

1. `getProducerForOverrides(Map<String, Object> producerOverrides)` — public entry point that
   layers caller-supplied producer properties (bootstrap servers, SSL/SASL, etc.) on top of the
   manager's own request-timeout/max-block defaults, then falls back to `kafka:29092` +
   `StringSerializer` if anything's still unset.
2. `buildProducerProperties(...)` — the properties-building helper backing it, `@VisibleForTesting`.

`orchestration-event-kafka-listener` then adds a `conductor-kafka` dependency and switches
`OrchestrationEventKafkaPublisher` to get its `Producer` from the shared `KafkaProducerManager`
instead of constructing its own `KafkaProducer` and closing it itself — lifecycle/caching now
belongs to the manager.

## Can this be done without touching the kafka module at all?

Technically yes, but not cleanly. The only method on `KafkaProducerManager` that's `public` before
this diff is:

```java
public Producer getProducer(KafkaPublishTask.Input input)
```

Everything else (`getFromCache`, `getProducerProperties`) is package-private, so it's inaccessible
from another module's package. To reuse the manager without any kafka-module changes, the listener
would have to fabricate a `KafkaPublishTask.Input` and call `getProducer(input)`. Downsides:

- **No arbitrary overrides.** `Input` only exposes `bootStrapServers`, `keySerializer`,
  `requestTimeoutMs`, `maxBlockMs`. The listener's README promises "any `ProducerConfig` key"
  (SSL, SASL, `client.id`, etc.) — none of that has a field to flow through, so it would silently
  get dropped.
- **No safe bootstrap-servers default.** `getProducerProperties(Input)` does
  `configProperties.put(BOOTSTRAP_SERVERS_CONFIG, input.getBootStrapServers())` with no
  null-guard. `Properties`/`Hashtable` throws `NullPointerException` on a null value, so if the
  listener's config doesn't set bootstrap servers, this throws instead of falling back to a shared
  default.
- **Semantic mismatch.** `KafkaPublishTask.Input` is a task DTO; repurposing it as a generic
  "producer config bag" from a workflow-status listener is a leaky abstraction that will confuse
  future readers.

So "use it unmodified" means giving up the exact things the current design goal calls for
(arbitrary producer overrides + safe defaults + shared caching).

## Recommendation

Keep the additive change: it's a new public method (`getProducerForOverrides`) plus a private
helper, doesn't touch `getProducer(Input)` or its existing behavior/tests at all, and
`KafkaProducerManager` is already a shared `@Component` bean designed to be depended on. That's a
low-risk, backward-compatible extension, not a modification of existing behavior.

If the real constraint is "no cross-module dependency on `conductor-kafka` at all," the
alternative isn't reuse — it's reverting to the listener building its own `KafkaProducer` (as
before), which drops the shared broker-config/caching benefit entirely.
