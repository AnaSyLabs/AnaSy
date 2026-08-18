# ClickHouse sink

Reference warehouse for ANASy. Optional. The core does not depend on this module.

Contract: [../lld.md](../lld.md). DDL: [clickhouse.sql](clickhouse.sql).

## Module

`sinks/clickhouse-batch-sink` — Spring Boot app. `group-id: anas-sink-clickhouse`. Package `io.anasy.sink.clickhouse`.

Dependencies: `spring-boot-starter`, `spring-kafka`, `spring-boot-starter-jdbc`, `com.clickhouse:clickhouse-jdbc`.

## Local

Add ClickHouse next to Kafka (Kafka compose stays in the LLD):

```yaml
  clickhouse:
    image: clickhouse/clickhouse-server:24.12
    ports: ["8123:8123", "9000:9000"]
    environment:
      CLICKHOUSE_DB: anas
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: anas
```

Apply [clickhouse.sql](clickhouse.sql). Overlay compose: `sinks/clickhouse-batch-sink/docker-compose.yml`. Full stack including DuckDB and the viewer: `examples/docker-compose.yml`.

## Listener

Spring JDBC auto-configures `DataSource` + `JdbcTemplate` from `spring.datasource.*`. No custom ClickHouse config class.

```java
package io.anasy.sink.clickhouse;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.stereotype.Component;

@Component
public class ClickHouseSinkListener {

    private static final String INSERT = """
            INSERT INTO fat_events
              (event_id, topic, kafka_partition, kafka_offset, event_ts, payload)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbc;
    private final DeadLetterPublishingRecoverer dlq;

    public ClickHouseSinkListener(JdbcTemplate jdbc, DeadLetterPublishingRecoverer dlq) {
        this.jdbc = jdbc;
        this.dlq = dlq;
    }

    @KafkaListener(topics = "${anas.sink.topics}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(List<ConsumerRecord<String, String>> records) {
        List<ConsumerRecord<String, String>> good = new ArrayList<>();
        for (var rec : records) {
            if (rec.key() == null || rec.key().isBlank()) {
                dlq.accept(rec, new PoisonEventException(
                        "missing eventId topic=%s partition=%d offset=%d"
                                .formatted(rec.topic(), rec.partition(), rec.offset())));
                continue;
            }
            good.add(rec);
        }
        if (good.isEmpty()) {
            return;
        }
        jdbc.batchUpdate(INSERT, good, good.size(), (ps, rec) -> {
            ps.setString(1, rec.key());
            ps.setString(2, rec.topic());
            ps.setInt(3, rec.partition());
            ps.setLong(4, rec.offset());
            ps.setTimestamp(5, Timestamp.from(Instant.ofEpochMilli(rec.timestamp())));
            ps.setString(6, rec.value() == null ? "" : rec.value());
        });
    }
}
```

Poison keys go to DLQ via `DeadLetterPublishingRecoverer`; they never hit JDBC. `event_date` is `DEFAULT toDate(event_ts)`. `ingested_at` is `DEFAULT now()`. Neither is bound in JDBC.

## `application.yml`

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: anas-sink-clickhouse
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      enable-auto-commit: false
      auto-offset-reset: earliest
      max-poll-records: 10000
      fetch-min-size: 1MB
      fetch-max-wait: 500ms
      properties:
        max.partition.fetch.bytes: 10485760
        max.poll.interval.ms: 600000
    listener:
      type: batch
      ack-mode: batch
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer

  datasource:
    url: jdbc:clickhouse:http://localhost:8123/anas
    username: default
    password: anas
    driver-class-name: com.clickhouse.jdbc.ClickHouseDriver
    hikari:
      maximum-pool-size: 4
      connection-timeout: 10000

anas:
  sink:
    name: clickhouse
    topics: orders.events,payments.events
    retry:
      initial-interval: 1s
      multiplier: 2.0
      max-interval: 60s
      max-attempts: 8
    dlq:
      suffix: .dlq.clickhouse
```

`max-poll-records: 10000` is ClickHouse-healthy (10k–100k rows/insert). Drop it for large payloads. Hikari stays small; raise `listener.concurrency` only up to partition count, and watch `system.parts`.

Retry/DLQ: same `DefaultErrorHandler` as the core sink contract in the LLD. This sink’s DLQ is `orders.events.dlq.clickhouse`, not a shared `.DLT`. ClickHouse timeouts are retryable; a missing Kafka key is not.

## Schema

Query patterns: `WHERE topic = ? AND event_date BETWEEN ? AND ?`; JSON extract; rare lookup by `event_id`.

```sql
CREATE DATABASE IF NOT EXISTS anas;

CREATE TABLE anas.fat_events
(
    event_id        UUID,
    topic           LowCardinality(String),
    kafka_partition UInt16 DEFAULT 0,
    kafka_offset    UInt64 DEFAULT 0,
    event_ts        DateTime,
    event_date      Date DEFAULT toDate(event_ts),
    payload         String DEFAULT '',
    ingested_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (topic, event_date, event_id)
TTL event_date + INTERVAL 13 MONTH DELETE
SETTINGS index_granularity = 8192;
```

| Column | Type | Why |
|---|---|---|
| `event_id` | `UUID` | Dedup key. Kafka key |
| `topic` | `LowCardinality(String)` | Few topics; leading `ORDER BY` |
| `kafka_partition` / `kafka_offset` | unsigned ints | Debug. Not the dedup key |
| `event_ts` | `DateTime` | Kafka timestamp |
| `event_date` | `Date` | Coarse key + partition + TTL |
| `payload` | `String` | Raw JSON. Opaque at ingest |
| `ingested_at` | `DateTime` | Replace version; last insert wins |

Replacement key is the `ORDER BY` tuple. Kafka redelivery keeps the original record timestamp, so `event_date` is stable.

TTL 13 months. Drop old data with `DROP PARTITION`, not `ALTER DELETE`.

Payload stays `String`, not ClickHouse `JSON`. Promote hot paths with a materialized column when a field is actually queried constantly. That is the first silver step; see [../medallion.md](../medallion.md).

Rules applied here (ClickHouse sink only): `schema-pk-plan-before-creation`, `schema-pk-cardinality-order`, `schema-pk-prioritize-filters`, `schema-types-native-types`, `schema-types-lowcardinality`, `schema-types-avoid-nullable`, `schema-json-when-to-use`, `schema-partition-lifecycle`, `insert-batch-size`, `insert-mutation-avoid-update`, `insert-optimize-avoid-final`.

## JSON extraction

Filter `topic` and `event_date` first.

```sql
SELECT
    event_id,
    event_ts,
    JSONExtractString(payload, 'customerName') AS customer,
    JSONExtractFloat(payload, 'totalAmount')   AS amount
FROM anas.fat_events
WHERE topic = 'orders.events'
  AND event_date >= today() - 7
LIMIT 10;
```

```sql
SELECT JSONExtractString(payload, 'customer', 'id') AS customer_id
FROM anas.fat_events
WHERE topic = 'orders.events'
  AND event_date = today()
LIMIT 10;
```

Latest version of a replaced event (narrow reads):

```sql
SELECT event_id, payload
FROM anas.fat_events FINAL
WHERE topic = 'orders.events'
  AND event_date = today()
  AND event_id = '11111111-1111-1111-1111-111111111111';
```

Reports on larger ranges:

```sql
SELECT event_id, argMax(payload, ingested_at) AS payload
FROM anas.fat_events
WHERE topic = 'orders.events'
  AND event_date >= today() - 1
GROUP BY event_id;
```

Hot aggregations: incremental MV, added when a query is actually hot.

```sql
ALTER TABLE anas.fat_events
    ADD COLUMN amount Float64 DEFAULT JSONExtractFloat(payload, 'totalAmount');
```

## Test

Listener binds `event_id` from `record.key()`, not `UUID.randomUUID()`. A null key is recovered to `*.dlq.clickhouse`; sibling records in the batch still insert.
