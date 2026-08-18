# ANASy docs plan

Locked choices for the framework. Write to these. Do not reopen them without a concrete failure.

## What ANASy is

ANASy (Analytics Sync) keeps OLTP services thin. A service writes its business row, builds a fat event (joins already resolved), and publishes it once to Kafka.

Kafka is the product boundary. Sinks are independent Kafka consumers. ClickHouse and DuckDB are two sinks, not the architecture.

```
OLTP  →  EventPublisher  →  Kafka  →  sink A (own group)
                              └────→  sink B (own group)
```

## Modules

| Module | Artifact | Layer |
|---|---|---|
| Producer starter | `event-connector-starter` | Core. Required. |
| ClickHouse sink | `clickhouse-batch-sink` | Optional warehouse under `sinks/` |
| DuckDB sink | `duckdb-batch-sink` | Optional warehouse under `sinks/` |
| Sample OLTP | `examples/starter-example` | Copy this pom. |
| Local viewer | `viewer/` | Demo dashboard. Not a sink. |
| Next warehouse | `sinks/<name>` | New Spring Boot app. Own `group-id`. |

No shared `sink-spi`, `SinkWriter`, or `anas.sink.type` switch. Kafka consumer groups are the plugin system.

## Locked (core — sink-agnostic)

| Topic | Choice | Why |
|---|---|---|
| Group / packages | `io.anasy` | Matches the product name |
| Java / Boot | Java 21, Spring Boot 3.4 | Current LTS + current Boot 3 |
| Build | Maven parent `anas` | Starter convention |
| Serialization | JSON via Jackson. Avro = swap `value-serializer` | No custom Avro stack |
| Envelope | None. Kafka key = `eventId`, timestamp = event time, value = fat JSON | One less wrapper; sinks do not share a DTO |
| Producer send | `publish` non-blocking. `publishAndWait` blocking. Caller chooses per call | OLTP stays thin by default. Block only when the request must fail if Kafka did not ack |
| `event_id` | Created in the producer, used as Kafka key | Every sink can idempotently upsert. Sink-generated UUIDs cannot |
| Fan-out | One topic, N consumer groups | Native Kafka. Two sinks both see every event |
| Sink shape | Separate Spring Boot app per destination | DuckDB copied the ClickHouse listener. Extract a helper only if a third copy hurts |
| Ack | `listener.ack-mode: batch` after a successful write | Failed write → no commit |
| Retry | Blocking `ExponentialBackOff` on the sink thread | Keeps the batch together. Pause that partition while the warehouse is sick |
| DLQ | Kafka topic `{topic}.dlq.{sink-name}` | Per sink. Spring DLT, not a shared queue, not `@RetryableTopic` |
| Schema Registry | Out of v1 | JSON payload does not need it |
| Extra core modules (`common`, `spi`, `admin`) | No | YAGNI |

## Locked (ClickHouse sink only)

These bind `clickhouse-batch-sink`. They do not bind ANASy.

| Topic | Choice |
|---|---|
| Engine | `ReplacingMergeTree(ingested_at)` |
| Payload | `String` (raw JSON) |
| `ORDER BY` | `(topic, event_date, event_id)` |
| `PARTITION BY` | `toYYYYMM(event_date)` |
| Insert | JDBC `batchUpdate`, 10k–100k rows |

## Locked (DuckDB sink only)

These bind `duckdb-batch-sink`. They do not bind ANASy.

| Topic | Choice |
|---|---|
| Store | File `anas.duckdb` |
| Dedup | `VARCHAR PRIMARY KEY` + `INSERT OR REPLACE` |
| Pool | Hikari size 1 (single writer) |
| Viewer reads | HTTP `/internal/*` on the sink (port 8081). Do not open the file from a second process |

DuckDB rules live in [sinks/duckdb.md](sinks/duckdb.md).


## Sink contract (every destination)

A sink is valid if it:

1. Uses a **unique consumer `group-id`**
2. Reads `eventId` from the Kafka **key** (never generates one)
3. Treats the value as an **opaque JSON string** unless that warehouse requires typed columns
4. Writes **durably, then returns** so offsets commit
5. Dedups on `event_id` using whatever the warehouse already has (PK, upsert, replace merge, `ON CONFLICT`)
6. Retries **transient** warehouse failures with exponential backoff, then DLQ
7. Sends **poison** (missing key, non-retryable) to that sink’s DLQ immediately, without stalling the good rows in the batch

How ClickHouse, DuckDB, Iceberg, or a blob store implements (5) is that sink’s problem.

## Retry and DLQ (locked)

| Choice | Value | Why |
|---|---|---|
| Mechanism | `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` | Spring Kafka already does this |
| Backoff | `ExponentialBackOffWithMaxRetries`: 1s × 2, cap 60s, 8 attempts | ~3 min of retries, then give up. Not `FixedBackOff` |
| Blocking vs retry-topics | Blocking | `@RetryableTopic` is per-record and shreds batch inserts |
| Retryable | Warehouse I/O, timeouts, SQL/resource failures | Destination is down; wait |
| Not retryable | Missing `eventId`, deserialization | Will never succeed. Immediate DLQ |
| Poison in a batch | Split: DLQ the bad keys, write the rest | One blank key must not DLQ 10k good rows |
| DLQ topic | `{topic}.dlq.{sink-name}` e.g. `orders.events.dlq.clickhouse` | Two sinks must not share a DLQ |
| DLQ partition | Same index as the source record | Replay stays ordered per partition |
| DLQ key | Original `eventId` | Idempotent replay |
| After DLQ | `commitRecovered=true` | Do not loop the poison |
| Replay | Republish to the **original** topic with the original key | Normal sink path + warehouse dedup. No replay service |
| Producer | Kafka `retries` + idempotent producer only | DLQ is a sink concern. Blocking publish throws to the OLTP caller; it is not a producer DLQ |
| `max.poll.interval.ms` | ≥ backoff budget + insert time (10 min) | Default 5 min can kick the consumer mid-backoff |

Not infinite retry. A 10-minute warehouse outage DLQs the in-flight batches; replay after it is healthy. A partition that retries forever is a silent outage.

## Doc map

| File | Contents |
|---|---|
| [hld.md](hld.md) | Architecture, fan-out, Kafka contract |
| [lld.md](lld.md) | Starter, publisher API, how to add a sink |
| [medallion.md](medallion.md) | Bronze (have) → silver/gold plan. ClickHouse modeling only |
| [sinks/clickhouse.md](sinks/clickhouse.md) | ClickHouse sink: config, code, DDL, queries |
| [sinks/clickhouse.sql](sinks/clickhouse.sql) | ClickHouse DDL |
| [sinks/duckdb.md](sinks/duckdb.md) | DuckDB sink: file store, PK upsert, query port |
| [sinks/duckdb.sql](sinks/duckdb.sql) | DuckDB DDL |
| [../examples/docker-compose.yml](../examples/docker-compose.yml) | Full local stack: Kafka, both sinks, sample OLTP, viewer |
| [../AGENTS.md](../AGENTS.md) | Ponytail + ANASy constraints |

No ADR folder. This file is the decision log.

## v1 non-goals

- A sink SPI or multi-writer process
- Exactly-once (Kafka transactions + warehouse)
- Parsing JSON in the core (the starter does not know the warehouse)
- Schema Registry, outbox
- A product admin / ops control plane. Local `viewer/` is a demo dashboard (where is this `event_id`), not a sink and not a control plane
- An app-wide `anas.publisher.blocking` switch (the method is the switch)
