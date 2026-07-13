# Enabling Quartz/Cron Scheduling for Conductor

Investigated 2026-07-13. The "Quartz scheduling" feature is the `scheduler` module
(`org.conductoross.conductor.scheduler`) — a DB-polling scheduler that evaluates
Quartz-syntax cron expressions (6-field, second precision) to fire workflow starts.
It is **not** the Quartz library itself; it's a custom polling loop that parses
Quartz-style cron strings.

## Module layout

- `scheduler/` — core auto-configuration, service, REST API, model
- `scheduler-postgres-persistence/` — Postgres DAO + Flyway migrations
- `scheduler-mysql-persistence/` — MySQL DAO + Flyway migrations

All three are declared in root `settings.gradle` (renamed to `conductor-scheduler`,
`conductor-scheduler-postgres-persistence`, `conductor-scheduler-mysql-persistence`).

**Only Postgres is wired into the server by default.**
`server/build.gradle:45`:
```groovy
implementation project(':conductor-scheduler-postgres-persistence')
```
There is no equivalent line for `conductor-scheduler-mysql-persistence` — if you need
MySQL-backed scheduling you must add that dependency yourself and rebuild.

## Activation gates (all must be true)

1. **`conductor.scheduler.enabled=true`**
   Gates `WorkflowSchedulerConfiguration` (`scheduler/.../config/WorkflowSchedulerConfiguration.java:33-38`)
   via `@ConditionalOnProperty(name = "conductor.scheduler.enabled", havingValue = "true", matchIfMissing = false)`.
   Default when unset: disabled.

2. **`conductor.db.type` must be `postgres` or `mysql`**
   The persistence auto-configs use `@ConditionalOnExpression`, not a simple property, combining both conditions:
   - `PostgresSchedulerConfiguration` (`scheduler-postgres-persistence/.../config/PostgresSchedulerConfiguration.java:34-37`):
     `"'${conductor.db.type:}' == 'postgres' && '${conductor.scheduler.enabled:false}' == 'true'"`
   - `MySQLSchedulerConfiguration` (`scheduler-mysql-persistence/.../config/MySQLSchedulerConfiguration.java:34-37`):
     `"'${conductor.db.type:}' == 'mysql' && '${conductor.scheduler.enabled:false}' == 'true'"`

   Server default (`server/src/main/resources/application.properties:17`) is
   `conductor.db.type=sqlite` — scheduler persistence never activates under the default profile.

3. Internally, `SchedulerService` bean requires both a `SchedulerDAO` bean (from whichever
   persistence module activated) and a `WorkflowService` bean (`@ConditionalOnBean`,
   `WorkflowSchedulerConfiguration.java:41-48`). The REST controller `SchedulerResource`
   only loads if `SchedulerService` exists (`:50-54`).

No properties file in the repo currently sets `conductor.scheduler.enabled=true` —
not even `docker/server/config/config-postgres.properties` (which does set
`conductor.db.type=postgres` at line 2). You must add it yourself.

## Steps to enable (Postgres path — default supported backend)

1. Ensure `server/build.gradle` depends on `conductor-scheduler-postgres-persistence`.
2. Use/point config at `docker/server/config/config-postgres.properties` (sets `conductor.db.type=postgres`), or set `conductor.db.type=postgres` in whatever properties file/env you use.
3. Add `conductor.scheduler.enabled=true` to that same config.
4. Boot the server. On startup, a dedicated Flyway instance (`flywayForScheduler` bean, `initMethod=migrate`) runs `V1__scheduler_tables.sql` (the only migration file) against the DB, tracked in its own `flyway_schema_history_scheduler` table — isolated from the app's main migration history, so no manual migration step is needed.
5. Manage schedules via REST, base path `/api/scheduler` (`SchedulerResource.java:46`):
   - `POST /api/scheduler/schedules` — create/update
   - `GET /api/scheduler/schedules/{name}` — get one
   - `GET /api/scheduler/schedules?workflowName=` — list/filter
   - `DELETE /api/scheduler/schedules/{name}` — delete
   - `PUT /api/scheduler/schedules/{name}/pause?reason=`
   - `PUT /api/scheduler/schedules/{name}/resume`
   - `GET /api/scheduler/schedules/{name}/executions?limit=10`
   - `GET /api/scheduler/schedules/{name}/next-execution-times?count=5`

   Full interactive API reference via Swagger UI: `http://<host>:<port>/swagger-ui/index.html`
   (OpenAPI spec served at `http://<host>:<port>/api-docs`, per `springdoc.api-docs.path=/api-docs`
   in `server/src/main/resources/application.properties:15`).
6. Or use the existing `ui-next` Scheduler UI (`ui-next/src/pages/scheduler/`, `ui-next/src/pages/definitions/Scheduler/`) — wired to these endpoints via `schedulerHooks.js`.

## Cron expression format

`WorkflowSchedule.cronExpression` (`scheduler/.../model/WorkflowSchedule.java:26-70`) is a
single string field: 6-field, second-precision Quartz cron syntax, e.g. `0 0 9 * * MON-FRI`.
Plus `zoneId` (default `UTC`), `paused`/`pausedReason`, `startWorkflowRequest`,
`scheduleStartTime`/`scheduleEndTime`, `runCatchupScheduleInstances`.

**Multi-cron-per-schedule is NOT supported on current `main`.** PR #914
("feat: support multi cron in scheduler") was merged then reverted by PR #926
(commits `94d569fa3` then `78a0812af` in this repo's history) — both are in the log,
net effect is single-cron-only today.

## Tunable properties (`SchedulerProperties.java:23-127`, prefix `conductor.scheduler.`)

| Property | Default |
|---|---|
| `conductor.scheduler.polling-thread-count` | `1` |
| `conductor.scheduler.polling-interval` | `100` (ms) |
| `conductor.scheduler.poll-batch-size` | `5` |
| `conductor.scheduler.scheduler-time-zone` | `UTC` |
| `conductor.scheduler.archival-max-records` | `5` |
| `conductor.scheduler.archival-max-record-threshold` | `10` |
| `conductor.scheduler.jitter-max-ms` | `0` |

## Gaps / things to watch

- **No user-facing docs exist.** `docs/` only documents the unrelated legacy
  `conductor.app.eventQueueSchedulerPollThreadCount` property. The only doc specific
  to this module is `scheduler/SCHEDULER_DAO_TEST_PLAN.md`, which is a test-strategy
  doc, not setup docs, and it's partly stale — it describes a
  `conductor-scheduler-sqlite-persistence` module that does not actually exist in
  the repo (not in `settings.gradle`, no such directory).
- **MySQL scheduling requires a manual build change** — add
  `implementation project(':conductor-scheduler-mysql-persistence')` to
  `server/build.gradle` yourself; it's not on the classpath by default.
- **No docker-compose file wires this up.** Neither root `docker-compose.yaml`/
  `docker-compose-local.yaml` nor anything under `docker/` sets
  `conductor.scheduler.enabled` or defines a scheduler-specific service — you must
  add the env var/property yourself on top of an existing Postgres-backed compose setup
  (`docker-compose-postgres.yaml` + `config-postgres.properties` is the closest base).
- The whole module (`scheduler`, `scheduler-postgres-persistence`,
  `scheduler-mysql-persistence`) was introduced in one commit,
  `25b7c8f1e feat: add conductor-scheduler-postgres-persistence module (#885)`,
  and is still under active follow-up (multi-cron add/revert happened after).
