h1. IndexDAO / elasticsearch7 vs elasticsearch:8.14.3 — Investigation Notes

{info:title=Status}
*Resolved.* No code change needed — the current uncommitted config is internally consistent and verified working against the ES 8.14.3 container.
{info}

h2. Question

Running conductor against {{docker.elastic.co/elasticsearch/elasticsearch:8.14.3}} with {{indexingBackend=elasticsearch7}} ({{gradle.properties}}) failed at startup:

{code:none}
Parameter 2 of constructor in com.netflix.conductor.core.dal.ExecutionDAOFacade required a bean of
type 'com.netflix.conductor.dao.IndexDAO' that could not be found.
{code}

h2. Root Cause of the IndexDAO Error

{{IndexDAO}} is only wired up if *two independent things agree*:

# *Build-time* — {{server/build.gradle}} reads the Gradle project property {{indexingBackend}} (root {{gradle.properties}}) to decide which persistence module gets compiled into {{conductor-server}}: {{conductor-es7-persistence}} or {{conductor-es8-persistence}}.
# *Run-time* — Spring {{@Conditional}}s in that module only activate the {{IndexDAO}} bean if the *application* properties match:
#* {{es7-persistence}}'s {{ElasticSearchV7Configuration}} requires {{conductor.indexing.enabled=true}} (default), {{conductor.indexing.type=elasticsearch}}, {{conductor.elasticsearch.version=7}}.
#* {{es8-persistence}}'s equivalent requires {{conductor.indexing.type=elasticsearch8}}.

{warning:title=Gotcha}
If these two layers disagree — e.g. the ES8 module was compiled in but the properties still say {{conductor.indexing.type=elasticsearch7}} / {{version=7}} (or vice versa) — *no* {{IndexDAO}} bean matches any {{@Conditional}}, and Spring fails with exactly the error above. This is also documented in {{conductor-oss-postgres-redis-es8-setup.md}} (repo root) as the #1 gotcha for the ES8 setup.
{warning}

h2. Verified: Current Uncommitted Config Is Internally Consistent and Works

Checked the three modified files together:

||File||Setting||Effect||
|{{docker-compose-local.yaml}}|ES image {{8.14.3}}|Runtime Elasticsearch server version|
|{{gradle.properties}}|{{indexingBackend=elasticsearch7}}|Compiles in {{conductor-es7-persistence}}|
|{{server/src/main/resources/config-local.properties}}|{{conductor.indexing.type=elasticsearch}}, {{conductor.elasticsearch.version=7}}|Satisfies {{ElasticSearchV7Enabled}}|

Ran it for real from {{server/}}:

{code:bash}
../gradlew bootRun -PindexingBackend=elasticsearch7 \
  --args='--spring.config.additional-location=file:./src/main/resources/config-local.properties'
{code}

...against the already-running {{elasticsearch-citi}} (8.14.3), {{postgres-citi}}, {{redis-citi}} containers:

* {{IndexDAO}} bean *was found* ({{com.netflix.conductor.es7.dao.index.ElasticSearchRestDAOV7}})
* It successfully created/verified all conductor indices ({{conductor_workflow}}, {{conductor_task}}, {{conductor_task_log_*}}, {{conductor_event_*}}, {{conductor_message_*}}) against the ES 8.14.3 server
* This works because ES 8.14.3 has {{minimum_wire_compatibility_version: 7.17.0}} — the ES7 module uses the version-agnostic low-level {{RestClient}} (plain HTTP), so it can talk to an ES 8.x server fine at this API surface.

My test run then failed at {{Failed to start bean 'webServerStartStop'}} — *port 8080 already bound*. That's unrelated to indexing: PID 66279 (started 13:17, IDE-launched, same classpath incl. {{conductor-es7-persistence}}) was already up and healthy:

{code:none}
GET /actuator/health → {"status":"UP", ... "elasticsearch":{"status":"UP", ...}}
{code}

h2. Conclusion

The {{IndexDAO}} error the user saw was from an earlier, inconsistent state (build-time backend and run-time properties not yet aligned). The current uncommitted diff already fixes it correctly — {{indexingBackend=elasticsearch7}} + {{conductor.elasticsearch.version=7}} + {{conductor.indexing.type=elasticsearch}} is a consistent combination and does work against the ES 8.14.3 container. *No code change needed* — an already-running instance on :8080 confirms it.

h2. Gotcha Checklist for Next Time This Breaks

* ☐ {{gradle.properties}} → {{indexingBackend}} (build-time, which persistence module compiles in)
* ☐ {{config-local.properties}} → {{conductor.indexing.type}} + {{conductor.elasticsearch.version}} (run-time, which {{@Conditional}} bean activates) — *must match item 1*
* ☐ Actually reload {{config-local.properties}} via {{--args='--spring.config.additional-location=file:./src/main/resources/config-local.properties'}} — {{application.properties}} defaults {{conductor.indexing.type=sqlite}}, so without this arg none of the ES {{@Conditional}}s match either.
* ☐ Check {{lsof -i :8080}} before starting a second instance — a duplicate on the same port fails with an unrelated {{webServerStartStop}} error that looks alarming but has nothing to do with indexing.