# DuckDB sink

Reference warehouse for ANASy. Optional. Independent of ClickHouse.

Contract: [../lld.md](../lld.md). DDL: [duckdb.sql](duckdb.sql).

## Module

`sinks/duckdb-batch-sink` — Spring Boot app. `group-id: anas-sink-duckdb`. Package `io.anasy.sink.duckdb`.

Dependencies: `spring-boot-starter`, `spring-kafka`, `spring-boot-starter-jdbc`, `org.duckdb:duckdb_jdbc`.

## Local

File-backed. No container. Default file `./data/anas.duckdb`. Override with `SPRING_DATASOURCE_URL=jdbc:duckdb:/data/anas.duckdb`.

Hikari pool size is 1. DuckDB is a single writer.

DDL is created on startup (`CREATE TABLE IF NOT EXISTS`). Same statement: [duckdb.sql](duckdb.sql).

## Listener

Copied from the ClickHouse sink loop. Poison keys go to DLQ; `event_id` is `record.key()`. Dedup is `INSERT OR REPLACE` on the `PRIMARY KEY`.

```java
INSERT OR REPLACE INTO fat_events
  (event_id, topic, kafka_partition, kafka_offset, event_ts, event_date, payload)
VALUES (?, ?, ?, ?, ?, ?, ?)
```

## `application.yml`

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: anas-sink-duckdb
      enable-auto-commit: false
      auto-offset-reset: earliest
    listener:
      type: batch
      ack-mode: batch
  datasource:
    url: jdbc:duckdb:./data/anas.duckdb
    driver-class-name: org.duckdb.DuckDBDriver
    hikari:
      maximum-pool-size: 1

server:
  port: 8081

anas:
  sink:
    name: duckdb
    topics: orders.events,payments.events
    dlq:
      suffix: .dlq.duckdb
```

Retry/DLQ: same `DefaultErrorHandler` as the core sink contract. This sink’s DLQ is `orders.events.dlq.duckdb`.

The sink also serves **read** queries for the local viewer on port `8081` (`/internal/health`, `/internal/events`, `/internal/events/{id}`). Do not open the DuckDB file from a second process.


## Schema

| Column | Type | Why |
|---|---|---|
| `event_id` | `VARCHAR PRIMARY KEY` | Kafka key. Idempotent replace |
| `topic` | `VARCHAR` | Stream name |
| `kafka_partition` / `kafka_offset` | ints | Debug |
| `event_ts` / `event_date` | timestamp / date | Event time |
| `payload` | `VARCHAR` | Raw JSON |
| `ingested_at` | timestamp | Last upsert wins via `INSERT OR REPLACE` |

## Test

Listener binds `event_id` from `record.key()`. A null key is recovered to `*.dlq.duckdb`; sibling records in the batch still upsert.
