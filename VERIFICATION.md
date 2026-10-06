# Consumer verification

## Final local verification — 2026-10-06

The report below this section is the historical consumer implementation snapshot. Its
not-yet-started standalone scope has been superseded by these actual executions.

From `/home/hooman/IdeaProjects/digiteen-assessment/wallet-event-consumer`:

```sh
mvn -s .mvn/settings.xml -gs .mvn/settings.xml -B -ntp verify
docker compose config --quiet
docker compose up --build -d --wait --wait-timeout 180
docker compose ps
docker compose exec -T wallet-event-consumer java -cp /app/health com.digiteen.consumer.HealthCheck
docker compose exec -T postgres psql -U consumer -d consumer -c 'SELECT version, description, success FROM flyway_schema_history ORDER BY installed_rank'
docker compose exec -T rabbitmq rabbitmqctl -q list_queues name messages_ready messages_unacknowledged --formatter=json
```

- Maven: BUILD SUCCESS, 9 tests, zero failures/errors/skips; packaged Boot JAR.
- Final standalone configuration/startup/probe/readbacks: all exit 0. All three services
  healthy; V1 `processed events` successful; durable audit queue exists, ready/unacknowledged 0/0.
- Standalone project is now `digiteen-event-consumer`, with zero published host ports.
  The original standalone PostgreSQL/RabbitMQ volume names are explicitly retained for data continuity.
- Initial startup failed because its generated PostgreSQL name collided with the integrated
  consumer PostgreSQL. Labels—not names—proved the conflicting database belongs to project
  `digiteen-wallet`, service `consumer-postgres`; it was left untouched.
- Only the failed standalone project's created Rabbit container and empty network were removed
  by exact IDs after checking Compose project/service/config-file labels and network attachments.
  No compose down, volume removal, force, pruning or broad cleanup ran.
- Readback confirmed every preexisting volume remains; the five integrated container IDs,
  StartedAt timestamps and mounts were unchanged and all healthy. Integrated HTTP smoke passed.
- Both standalone and integrated stacks are left running; their brokers/databases are intentionally
  isolated. A standalone consumer is not itself a publisher/consumer integration proof.
- Two live integrated rounds passed: all 50 expected events consumed per round, no repeated
  durable effects, >30-second consumer downtime recovery, settled publication markers and queue.
  Maximum normal latency was 0.424654 s and 0.345332 s. Full exact commands/checklist are in
  `../wallet-service/FINAL_ACCEPTANCE.md`; the proof runs from wallet-service.
- New failed-write logs retain original trace/event/transaction IDs while omitting exception
  text/payload. The regression failed before the change and the final full suite passed.

Generated evidence (ignored): `target/final-consumer-verify.log`, Surefire reports,
`target/final-standalone-compose.log` (original failed startup),
`target/final-standalone-compose-fixed.log`, `target/startup-resource-baseline.json`, and
`target/standalone-startup-proof.json`.

Expected nonblocking startup warnings: absent buildx uses the default builder; preserved volumes
have the old Compose project's labels. No final test failed/skipped. No push or remote was added.
Repository URL delivery still requires authorization; single-broker/poison-message/production
limits below remain explicit, and physical process-crash boundaries were not exhaustively tested.

## Historical implementation verification

Local verification on 2026-10-05 used Java 25.0.4.1, Maven 3.9.12 and Docker Engine 29.8.0.
Every Maven command used the repository's empty settings as **both** user/global settings;
no corporate Maven configuration or service dependencies were imported.

```sh
mvn -s .mvn/settings.xml -gs .mvn/settings.xml -B -ntp verify
docker compose config --quiet
docker build -t digiteen-wallet-event-consumer:verification .
docker image inspect digiteen-wallet-event-consumer:verification --format '{{json .Config.User}} {{json .Config.Healthcheck.Test}}'
docker run --rm --entrypoint java digiteen-wallet-event-consumer:verification -cp /app/health com.digiteen.consumer.HealthCheck
```

| Check | Observed result |
| --- | --- |
| Maven verify | **BUILD SUCCESS; 9 tests, 0 failures, 0 errors, 0 skipped**; executable Boot JAR packaged |
| `ConsumerIntegrationTest` | 5 tests passed with real pinned PostgreSQL and RabbitMQ Testcontainers |
| `EventListenerTest` | 3 tests passed |
| `HealthCheckTest` | 1 test passed |
| Compose parsing | Passed; parsed JSON asserted project `digiteen-wallet-consumer`, exactly the three expected services, and no published host ports |
| Docker build | Passed using the pinned Maven/JRE stages; verification image created |
| Runtime image configuration | User `10001:10001`; health command `java -cp /app/health com.digiteen.consumer.HealthCheck` |
| Runtime probe with no HTTP server | Exit code **1**, expected failure-closed behavior; standalone class loads in the JRE image |
| Structured console logs | Real committed/duplicate JSON lines include `eventId`, `transactionId`, `traceId`, `type`; no JSON encoder failures after correction |
| Poison payload logging | Test marker did not appear in captured console output; listener error appender recorded no throwable/payload |

## Behavioral evidence

- Eight concurrent transactional inserts using one event ID produced exactly one `true`
  return, seven duplicates and one committed database row.
- Publishing the same persistent JSON message twice through the real direct exchange produced
  one audit row, `consumption_committed` and `consumption_duplicate`. Rabbit management
  statistics reached zero total/ready/unacknowledged messages after successful consumption.
- During the committed-log appender callback, a fresh JDBC query could already see the row:
  success logging occurs after the service proxy transaction has returned/committed.
- A deterministic PostgreSQL `BEFORE INSERT` trigger raised SQLSTATE `08006` after advancing
  a nontransactional sequence. The sequence proved at least two failed processing attempts.
  After stopping the listener, the audit table was empty, the queue had one ready message and
  zero unacknowledged messages, and neither success nor duplicate logs had appeared. Removing
  the trigger and restarting normal consumption yielded one committed row and an empty queue.
  This proves failed database writes are not successfully acknowledged; **it does not claim a
  physical database-process outage test**.
- A malformed real Rabbit message was rejected without requeue or audit insertion. There is no
  DLQ, so poison messages are discarded; this limitation is explicit in the README.
- Valid DEPOSIT/WITHDRAWAL/TRANSFER events round-trip through Jackson 3. Invalid sides/types,
  unsupported versions, malformed/null/trailing JSON and null/fractional/string amounts never
  reach the transactional service.
- Unit tests cover committed/duplicate logging, propagation of database exceptions, no success
  log on a failed write, restoration of existing MDC and removal of newly scoped MDC.
- Live broker management API assertions verify a durable queue, durable direct exchange and
  the exact routing-key binding. The live application HTTP endpoint returns healthy without
  revealing components; `/actuator/info` returns 404.
- Probe unit tests verify HTTP 200 succeeds, HTTP 503 fails, and a stopped server fails.

## Issues found and corrected

- Boot 4 Logstash JSON formatting rejects a key emitted through both MDC and fluent key-values.
  `traceId` is now emitted **only by MDC**, with other IDs/type as fluent key-values.
- Testcontainers 1.21.4 needs `asCompatibleSubstituteFor` when using the literal tag-plus-digest
  image names; explicit image compatibility is declared without removing digest pins.
- Rabbit publishing and queue statistics are asynchronous; tests use bounded eventual
  assertions, not immediate queue-count assumptions.

Non-blocking tool warnings: Maven's Guava uses a deprecated Unsafe method; Mockito's dynamic
agent attachment warns on Java 25; Testcontainers' JUnit extension reports its legacy
`CloseableResource` API. The intentional database-failure test also emits Hikari connection
failure diagnostics. None prevented execution or skipped tests.

## Not executed here / parent-owned verification

- **The standalone Compose stack was not started**, as requested. Only parsing, image build
  and an isolated no-server health-probe invocation were executed.
- The wallet service, wallet-owned integration override, publisher-first startup and
  cross-service proofs are outside this repository's verification scope.
- No changes were made to the wallet repository; no push was performed.

For integrated execution, from this repository with the sibling override available:

```sh
docker compose -f ../wallet-service/compose.yml -f ../wallet-service/compose.integration.yml up --build -d
```

This uses one wallet-owned project/network/broker plus an independent consumer PostgreSQL.
Running both standalone stacks would instead create two disconnected brokers.

Raw local Maven evidence is generated under ignored `target/` (Surefire XML and
`verify-run.log`); image/config verification logs were kept in scratch storage, not committed
as machine-specific build artifacts.
