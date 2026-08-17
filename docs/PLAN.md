# ANASy docs plan

Locked choices for the framework. Write to these. Do not reopen them without a concrete failure.

## What ANASy is

ANASy (Analytics Sync) keeps OLTP services thin. A service writes its business row, builds a fat event (joins already resolved), and publishes it. Kafka buffers. A batch sink inserts raw JSON into ClickHouse. OLAP reads ClickHouse, not the OLTP database.

Two modules. Nothing else in v1.

| Module | Artifact | Role |
|---|---|---|
| Producer starter | `event-connector-starter` | Auto-configured `EventPublisher` → Kafka |
| Consumer app | `clickhouse-batch-sink` | Batch `@KafkaListener` → `JdbcTemplate.batchUpdate` |

## Locked decisions

| Topic | Choice | Why |
|---|---|---|
| Group / packages | `io.anasy` | Matches the product name |
| Java / Boot | Java 21, Spring Boot 3.4 | Current LTS + current Boot 3 |
| Build | Maven parent `anas`, two modules | Starter convention; Gradle adds nothing |
| Serialization | JSON via Jackson. Avro = swap `value-serializer` | No custom Avro stack |
| Envelope | None. Kafka key = `eventId`, timestamp = event time, value = fat JSON | One less wrapper |
| `event_id` | Generated in the producer, used as Kafka key | Sink-generated UUIDs break idempotency |
| ClickHouse engine | `ReplacingMergeTree(ingested_at)` | Kafka is at-least-once; collapse dupes on merge |
| Payload column | `String` (raw JSON text) | Schema evolves in OLAP, not in the sink. `JSONExtract*` for queries |
| `ORDER BY` | `(topic, event_date, event_id)` | Low → high cardinality; matches `WHERE topic` + time range |
| `PARTITION BY` | `toYYYYMM(event_date)` | Monthly lifecycle; ~12 partitions/year |
| Insert path | Client-side batches of 10k–100k rows over official JDBC | ClickHouse part health. Async insert is a fallback, not the default |
| Ack | Spring `listener.ack-mode: batch` after the listener returns | Failed insert → no commit → Kafka retry |
| ClickHouse Kafka engine | Out of v1 | The sink *is* the consumer; a second ingest path is duplication |
| Schema Registry | Out of v1 | JSON String payload does not need it |
| Extra modules (`common`, `avro`, `admin`) | No | YAGNI |

## ClickHouse rules applied

- `schema-pk-plan-before-creation` — `ORDER BY` chosen from query patterns before `CREATE TABLE`
- `schema-pk-cardinality-order` — `topic` (low) before `event_date` before `event_id` (high)
- `schema-pk-prioritize-filters` — queries filter topic + time
- `schema-types-native-types` / `schema-types-lowcardinality` — `UUID`, `DateTime`, `LowCardinality(String)` for `topic`
- `schema-types-avoid-nullable` — defaults, not `Nullable`
- `schema-json-when-to-use` — payload stays `String` because it is an opaque blob at ingest; extract in queries or promote hot paths later
- `schema-partition-lifecycle` / `schema-partition-low-cardinality` — monthly partitions for TTL / `DROP PARTITION`
- `insert-batch-size` — 10k–100k rows per `INSERT`
- `insert-mutation-avoid-update` — `ReplacingMergeTree`, never `ALTER UPDATE` for dupes
- `insert-optimize-avoid-final` — no `OPTIMIZE TABLE FINAL` in the sink
- `query-mv-incremental` — raw table first; MVs only for repeated aggregations

## Query patterns the schema serves

1. `WHERE topic = ? AND event_date BETWEEN ? AND ?` — primary
2. JSON field extract on recent topic slices — secondary
3. Point lookup by `event_id` — rare; allowed, not optimized

## Doc map

| File | Audience | Contents |
|---|---|---|
| [hld.md](hld.md) | Anyone joining | Context, containers, Mermaid, data flow, what is not in v1 |
| [lld.md](lld.md) | Implementers | Modules, auto-config, publisher API, sink, `application.yml`, schema, extract queries |
| [scaling.md](scaling.md) | Operators | Kafka partitions, batch tuning, idempotency, parts monitoring |
| [schema.sql](schema.sql) | ClickHouse | Runnable DDL |
| [../AGENTS.md](../AGENTS.md) | Agents | Ponytail ladder + ANASy constraints |
| [../README.md](../README.md) | Humans | Short index |

No ADR folder. This file is the decision log.

## v1 non-goals

- Exactly-once (Kafka transactions + CH is not worth it; at-least-once + replace is)
- ClickHouse Cloud-specific APIs
- Multi-tenant row filters
- Admin UI, metrics dashboards, schema registry
- Parsing JSON in the sink before insert
