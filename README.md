# Wallet event consumer

Independent Java 25 / Spring Boot 4.0.6 / Maven 3.9.12 service. Consumes successful
wallet-operation events and stores an audit record in its **own PostgreSQL database**.
It does not update balances, call the wallet API, or share the wallet database or code.

Executed acceptance evidence: [VERIFICATION.md](VERIFICATION.md). The combined checklist and
live delivery results are committed in the wallet repository's
[FINAL_ACCEPTANCE.md](https://github.com/HoomanDevp/digiteen-wallet-service/blob/main/FINAL_ACCEPTANCE.md)
(available after private publication and access; locally: `../wallet-service/FINAL_ACCEPTANCE.md`).

## Checkout layout for reviewers

After private publication and access are available, use explicit local directory names:

```sh
mkdir -p digiteen-assessment
cd digiteen-assessment
git clone https://github.com/HoomanDevp/digiteen-wallet-service.git wallet-service
git clone https://github.com/HoomanDevp/digiteen-wallet-event-consumer.git wallet-event-consumer
cd wallet-event-consumer
```

Any writable parent directory works. The integrated paths below use this sibling layout,
not machine-specific absolute paths. Standalone consumer startup needs only this repository.

## Run modes

### Isolated consumer development stack

From this repository:

```sh
docker compose up --build -d --wait --wait-timeout 180
docker compose ps
docker compose logs wallet-event-consumer
docker compose exec wallet-event-consumer java -cp /app/health com.digiteen.consumer.HealthCheck
docker compose exec postgres psql -U consumer -d consumer -c 'SELECT * FROM processed_events;'
docker compose down
```

Project name: `digiteen-event-consumer`. It creates a PostgreSQL instance, RabbitMQ,
and the consumer on its own default network. **No host ports are published**, including
management and health ports. PostgreSQL and RabbitMQ use persistent named volumes;
`docker compose down` preserves them (`down -v` deliberately deletes their data).
Application runs as UID/GID `10001:10001`; the JRE-only HTTP health probe needs no curl/wget.

The distinct project name prevents generated container names from colliding with the integrated
`digiteen-wallet` project. The standalone volume names remain explicitly pinned to
`digiteen-wallet-consumer_consumer-postgres` and `digiteen-wallet-consumer_consumer-rabbitmq`
to retain existing data; neither is the integrated consumer database volume.

Compose includes **public development-only credentials**, not production secrets:
PostgreSQL database/user `consumer`, password `local-consumer-development-only`;
RabbitMQ user `wallet`, password `local-rabbit-development-only`. Replace these with
secret injection, least-privilege database roles, TLS and broker access controls for production.
This local stack is not a hardened production deployment.

### Integrated wallet publisher + consumer

With sibling `../wallet-service` and its integration override present, run **from this repo**:

```sh
docker compose -f ../wallet-service/compose.yml -f ../wallet-service/compose.integration.yml up --build -d --wait --wait-timeout 180
docker compose -f ../wallet-service/compose.yml -f ../wallet-service/compose.integration.yml ps
docker compose -f ../wallet-service/compose.yml -f ../wallet-service/compose.integration.yml down
```

The wallet-owned project `digiteen-wallet` adds `consumer-postgres` (independent database)
and `wallet-event-consumer` to the wallet stack's default network and **one shared RabbitMQ**.
No manual network creation is needed. The publisher implementation and integration override
are owned by the wallet repository, not this repository.

**Do not launch both standalone Compose stacks and call that integration:** they own two
disconnected brokers. Choose integrated mode for end-to-end publishing. Assessment scope is
one wallet-service publisher instance; no multi-publisher coordination is claimed here.

### Local JVM / externally provisioned dependencies

```sh
mvn -s .mvn/settings.xml -gs .mvn/settings.xml -B -ntp package
# Supply DATABASE_PASSWORD and RABBITMQ_PASSWORD through the environment first.
java -jar target/wallet-event-consumer-0.1.0.jar
```

| Environment variable | Default / meaning |
| --- | --- |
| `DATABASE_URL` | `jdbc:postgresql://localhost:5432/consumer` |
| `DATABASE_USERNAME` | `consumer` |
| `DATABASE_PASSWORD` | Required; no application password default |
| `RABBITMQ_HOST` | `localhost` |
| `RABBITMQ_PORT` | `5672` |
| `RABBITMQ_USERNAME` | `wallet` |
| `RABBITMQ_PASSWORD` | Required; no application password default |

Health is `GET /actuator/health` on internal port 8080; only the health management
endpoint is exposed, without dependency details. Health includes database/broker checks.
Flyway applies the forward SQL migration on startup. `processed_events` is the only business
table; Flyway also owns its migration metadata table.

## Event contract and durable routing

Both applications independently declare the same durable topology (no shared library):

- Direct exchange: `wallet.events`
- Queue: `wallet.events.audit`
- Routing key: `wallet.operation.succeeded`

Either application can create the topology first. The listener takes a raw AMQP `Message`
and explicitly decodes its body with Jackson 3 (`tools.jackson`); it never relies on
Java type-name headers, Java serialization, or the publisher's DTO class.

Version-1 example (positive integer wallet-units, not floating-point values or a currency conversion):

```json
{
  "eventId": "a8117cbe-2744-4ca7-a9c1-9ab76a594a46",
  "schemaVersion": 1,
  "transactionId": 42,
  "transactionType": "DEPOSIT",
  "amount": 100,
  "sourceWalletId": null,
  "destinationWalletId": 1,
  "occurredAt": "2026-01-01T12:00:00Z",
  "traceId": "e3c6b1c8-f779-4fb3-bb8b-ff1977dba53d"
}
```

IDs for events/traces must be UUIDs; transaction ID, amount and populated wallet IDs must
be positive. `schemaVersion` must be 1. `DEPOSIT` requires only a destination,
`WITHDRAWAL` only a source, `TRANSFER` two different wallets. Missing required data,
string/fractional integer coercion, trailing JSON, invalid types and unsupported versions
are rejected before insertion. Future event schemas require explicit compatibility work.

## Commit, acknowledgment and deduplication

1. Decode and validate.
2. Call the separate `ProcessedEventService` Spring `@Transactional` proxy.
3. `INSERT ... ON CONFLICT (event_id) DO NOTHING` atomically inserts or detects a duplicate.
4. The transaction interceptor **commits before the service call returns**.
5. Log `consumption_committed` or `consumption_duplicate`, then return from the listener.
6. Rabbit listener container **AUTO** acknowledgment follows the listener's successful return.

Database/commit failures propagate; `default-requeue-rejected=true` keeps valid failed messages
retryable. There is no custom retry framework. Normal broker reconnection is Spring AMQP's
connection/container recovery. Prefetch is 10; concurrency is 1. The explicit factory pins
these settings, rather than relying on defaults.

This is **at-least-once delivery with event-ID deduplication**, not distributed exactly-once
execution: crashing after database commit but before Rabbit acknowledgment causes a redelivery
that creates no second audit row. The primary key also protects concurrent duplicate inserts.
Producer event IDs must be immutable and globally unique; a conflicting body reusing an existing
ID is treated as a duplicate, not reconciled. Retain deduplication rows for the required replay
window. No business foreign keys cross database ownership boundaries.

**Poison-message limitation:** malformed/unsupported messages throw
`AmqpRejectAndDontRequeueException`. No dead-letter queue is configured, so such rejected
messages are discarded. Production needs a deliberate DLQ/quarantine and operator replay
policy before depending on poison-message retention. Prolonged database write failures can
cause repeated immediate redelivery; stop consumption during maintenance instead of silently
acknowledging or treating database errors as poison.

## Logging and tests

Boot's Logstash JSON console format records `eventId`, `transactionId`, `traceId`, `type`
for committed/duplicate consumption. Fluent SLF4J fields carry business IDs; `traceId` comes
from scoped MDC and is restored/removed afterward. **Do not emit the same key through both
MDC and fluent key-values:** Boot 4's JSON writer rejects duplicate names. Error logs retain
only a safe error class, never failed AMQP bodies or parser exception text. The listener's
custom container error handler avoids Spring AMQP's default failed-message payload dump.
Validated event write failures log `consumption_failed` with the original trace/event/transaction
IDs before MDC is restored, then rethrow so the broker can retry.
Do not enable framework payload/debug logging on sensitive traffic.

```sh
mvn -s .mvn/settings.xml -gs .mvn/settings.xml -B -ntp verify
```

Docker is required: Testcontainers 1.21.4 runs pinned real PostgreSQL 18.3 and RabbitMQ
4.2.5-management images with ephemeral containers/ports. Integration tests are in the normal
Maven test phase (no optional profile, no silent Docker-unavailable skip). Tests prove
concurrent duplicate insertion, real Rabbit duplicate delivery, SQL write failure with no
successful ack and later recovery, and poison rejection. The failure test uses a PostgreSQL
trigger plus a nontransactional sequence to prove repeated attempted writes; it is not a
claim of a physical database-process outage. See [VERIFICATION.md](VERIFICATION.md).
