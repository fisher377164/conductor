h1. Running Conductor OSS 3.23.0 Locally — Postgres + Redis + Elasticsearch 8

This guide covers building Conductor OSS {{v3.23.0}} from source and running it locally with:

* *PostgreSQL* — workflow metadata storage
* *Redis* — task queue + distributed locking
* *Elasticsearch 8* — indexing/search

{info}
It includes every fix discovered while getting this stack working, in order.
{info}

----

h2. 1. Prerequisites

* Docker Desktop
* JDK 21+
* Node.js 18+ and Yarn (only needed if you also want to build the UI)
* Git

----

h2. 2. Clone and Checkout the Version

{code:bash}
git clone https://github.com/conductor-oss/conductor
cd conductor
git checkout v3.23.0
{code}

----

h2. 3. Find the Exact Elasticsearch Client Version Conductor Expects

Conductor's {{es8-persistence}} module is built against a *specific pinned version* of the {{co.elastic.clients}} Java API client. Using a mismatched ES server version causes a hard failure at startup ({{MissingRequiredPropertyException}}), so check this before doing anything else:

{code:bash}
grep -i "elasticsearch-java\|elastic.clients" es8-persistence/build.gradle
{code}

This will show something like:

{code:none}
implementation "co.elastic.clients:elasticsearch-java:${revElasticSearch8}"
{code}

Resolve the variable:

{code:bash}
grep -rn "revElasticSearch8" --include="*.gradle" .
{code}

For {{v3.23.0}}, this resolves to:

{code:none}
revElasticSearch8 = '8.19.11'
{code}

{warning:title=Use this exact version}
Use this exact version for your Elasticsearch Docker image. If you're on a different Conductor commit/tag, re-run this check — the pinned version can change.
{warning}

----

h2. 4. Docker Compose — Postgres, Redis, Elasticsearch 8

Create {{docker-compose-local.yaml}} in the project root:

{code:yaml}
services:
  postgres-citi:
    image: postgres:15
    environment:
      POSTGRES_USER: conductor
      POSTGRES_PASSWORD: password
      POSTGRES_DB: conductor
    ports:
      - "5433:5432"

  redis-citi:
    image: redis:7
    ports:
      - "6377:6379"

  elasticsearch-citi:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.19.11
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
    ports:
      - "9201:9200"
{code}

{note}
The {{version: "3.8"}} key is obsolete in modern Docker Compose and has been omitted — including it just produces a harmless warning.

Port mappings are {{HOST:CONTAINER}}. The container-internal ports ({{5432}}, {{6379}}, {{9200}}) are fixed by each image and don't change — only the host-side ports were remapped here (to {{5433}}, {{6377}}, {{9201}}) to avoid clashing with any local installs of Postgres/Redis/ES you might already have running.
{note}

Start everything:

{code:bash}
docker compose -f docker-compose-local.yaml up -d
{code}

Verify each service is reachable:

{code:bash}
pg_isready -h localhost -p 5433
redis-cli -p 6377 ping
curl http://localhost:9201/_cluster/health?pretty
{code}

{info}
The ES health check response should include a {{unassignedPrimaryShards}} field — if it's missing, your ES image version doesn't match {{revElasticSearch8}} and the Java client will fail to deserialize the response at startup.
{info}

h3. Troubleshooting: Docker Pull Fails with a Digest Mismatch

{code:none}
failed commit on ref "layer-sha256:...": commit failed: unexpected commit digest ...
{code}

This is a corrupted/incomplete image layer cache, not a config issue.

{code:bash}
docker compose -f docker-compose-local.yaml down
docker rmi postgres:15 -f 2>/dev/null
docker system prune -f
docker compose -f docker-compose-local.yaml pull
docker compose -f docker-compose-local.yaml up -d
{code}

If it recurs on the same layer, disable containerd image storage: *Docker Desktop → Settings → General → uncheck "Use containerd for pulling and storing images"* → Apply & Restart → retry.

----

h2. 5. Gradle Build-Time Property: {{indexingBackend}}

{warning:title=This is the fix most people miss}
Which Elasticsearch persistence module gets compiled into {{conductor-server}} is decided at *build time* by a Gradle project property, not by anything in {{application.properties}}.
{warning}

{code:none}
// server/build.gradle
def indexingBackend = project.findProperty('indexingBackend') ?: 'elasticsearch'
...
} else if (indexingBackend == 'elasticsearch8' || indexingBackend == 'es8') {
    implementation project(':conductor-es8-persistence')
} else if (indexingBackend == 'elasticsearch7' || indexingBackend == 'es7' || indexingBackend == 'elasticsearch') {
    implementation project(':conductor-es7-persistence')
}
{code}

It *defaults to {{elasticsearch7}}* if unset. Setting {{conductor.indexing.type=elasticsearch8}} in your properties file alone does nothing if the ES8 module was never compiled in — you'll get:

{code:none}
Parameter 2 of constructor in ExecutionDAOFacade required a bean of type 'IndexDAO' that could not be found
{code}

*Fix — set the Gradle property.* Add to {{gradle.properties}} in the project root (create it if missing):

{code:properties}
indexingBackend=elasticsearch8
{code}

Then re-sync Gradle (IntelliJ: elephant icon → refresh, or *File → Sync Project with Gradle Files*).

Alternative, one-off ways to set it:

{code:bash}
./gradlew :conductor-server:bootRun -PindexingBackend=elasticsearch8 ...
{code}

...or in an IntelliJ Run Configuration's *Arguments* field:

{code:none}
-PindexingBackend=elasticsearch8
{code}

----

h2. 6. {{config-local.properties}}

Create {{server/src/main/resources/config-local.properties}}:

{code:properties}
# --- Database: PostgreSQL (host port 5433) ---
conductor.db.type=postgres
spring.datasource.url=jdbc:postgresql://localhost:5433/conductor
spring.datasource.username=conductor
spring.datasource.password=password

# --- Queue: Redis (host port 6377) ---
conductor.queue.type=redis_standalone
conductor.redis.hosts=localhost:6377:us-east-1c
conductor.redis.workflowNamespacePrefix=conductor
conductor.redis.queueNamespacePrefix=conductor_queues

# --- Locking: Redis (host port 6377) ---
conductor.app.workflowExecutionLockEnabled=true
conductor.workflow-execution-lock.type=redis
conductor.redis-lock.serverType=SINGLE
conductor.redis-lock.serverAddress=redis://localhost:6377

# --- Indexing: Elasticsearch 8 (host port 9201) ---
conductor.indexing.enabled=true
conductor.indexing.type=elasticsearch8
conductor.elasticsearch.url=http://localhost:9201
conductor.elasticsearch.version=8
conductor.elasticsearch.indexName=conductor
conductor.elasticsearch.clusterHealthColor=yellow
{code}

{note}
The {{us-east-1c}} value in {{conductor.redis.hosts}} is just a rack/zone label the Redis client expects in {{host:port:rack}} format — it doesn't need to be a real AWS zone for local dev. Rename to {{local}} if you prefer.
{note}

----

h2. 7. Build and Run

{code:bash}
./gradlew build -x test
{code}

Run via Gradle CLI:

{code:bash}
cd server
../gradlew bootRun -PindexingBackend=elasticsearch8 \
  --args='--spring.config.additional-location=file:./src/main/resources/config-local.properties'
{code}

Or via IntelliJ's {{bootRun}} task/Run Configuration — make sure both are set:

* *Program arguments / {{--args}}*: {{--spring.config.additional-location=file:./src/main/resources/config-local.properties}}
* *Gradle project property*: {{-PindexingBackend=elasticsearch8}} (in the run config's Arguments field, or already present via {{gradle.properties}})

----

h2. 8. Verify Conductor Is Up

* UI (if built, see below): {{http://localhost:8080}}
* Swagger: {{http://localhost:8080/swagger-ui/index.html}}

----

h2. 9. (Optional) Build the UI

{{v3.23.0}} uses the legacy {{/ui}} React app (not the newer {{ui-next}} track).

*Dev mode* (hot reload, proxies to your running Conductor server on {{8080}}):

{code:bash}
cd ui
yarn install
yarn run start
{code}

Runs at {{http://localhost:5000}}.

*Production build* (bundles UI into the server, served from {{8080}} directly):

{code:bash}
cd conductor
./build_ui.sh
cd server
../gradlew bootRun -PindexingBackend=elasticsearch8 ...
{code}

----

h2. 10. Known Gotchas Summary

||Symptom||Cause||Fix||
|{{IndexDAO could not be found}}|{{es8-persistence}} module not compiled in|Set {{-PindexingBackend=elasticsearch8}} (Gradle property, not app config)|
|{{MissingRequiredPropertyException: unassignedPrimaryShards}}|ES server version ≠ client version Conductor expects|Pin Docker image to {{revElasticSearch8}} value found in {{build.gradle}}|
|Docker pull {{commit digest mismatch}}|Corrupted image layer cache|{{docker system prune -f}} and retry; disable containerd image store if it persists|
|Gradle build fails in {{grpc}} module (Apple Silicon)|Missing {{osx-x86_64}} classifier|Add {{:osx-x86_64}} to {{protoc}}/{{grpc}} artifact lines in {{grpc/build.gradle}}|
|Obsolete {{version}} warning in {{docker compose}}|Compose v2 no longer needs the {{version}} key|Remove the {{version: "3.8"}} line from the compose file|