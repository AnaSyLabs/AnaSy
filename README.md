# ANASy

Analytics Sync. Keep OLTP thin: publish fat events to Kafka, batch-insert raw JSON into ClickHouse.

```
OLTP  --EventPublisher-->  Kafka  --batch sink-->  ClickHouse fat_events
```

## Modules

| Module | What |
|---|---|
| `event-connector-starter` | Spring Boot starter. Inject `EventPublisher`, send JSON. |
| `clickhouse-batch-sink` | `@KafkaListener` (batch) + `JdbcTemplate.batchUpdate`. |

Java 21, Spring Boot 3.4, Maven. Group `io.anasy`.

## Docs

| Doc | Read when |
|---|---|
| [docs/PLAN.md](docs/PLAN.md) | Why these choices are locked |
| [docs/hld.md](docs/hld.md) | Architecture (Mermaid) |
| [docs/lld.md](docs/lld.md) | Setup, config, code, schema, queries |
| [docs/scaling.md](docs/scaling.md) | Partitions, batches, idempotency |
| [docs/schema.sql](docs/schema.sql) | ClickHouse DDL |
| [AGENTS.md](AGENTS.md) | Agent rules (Ponytail + ANASy) |

## Quick path

1. Run Kafka + ClickHouse (compose sketch in the LLD).
2. Apply `docs/schema.sql`.
3. Depend on `event-connector-starter` in the OLTP service.
4. Run `clickhouse-batch-sink`.

```java
events.publish("orders.events", order.id().toString(), fatEvent);
```

Producer generates `eventId` (the Kafka key). The sink never invents ids. Duplicates collapse in `ReplacingMergeTree`.
