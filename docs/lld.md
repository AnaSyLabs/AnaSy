# ANASy low-level design

Implements [hld.md](hld.md). Decisions in [PLAN.md](PLAN.md). Runnable DDL: [schema.sql](schema.sql).

## Module structure

Maven parent `anas` (`io.anasy:anas`). Two modules. No `common`.

```
anas/
├── pom.xml
├── event-connector-starter/
│   ├── pom.xml
│   └── src/main/
│       ├── java/io/anasy/connector/
│       │   ├── EventPublisher.java
│       │   ├── KafkaEventPublisher.java
│       │   ├── EventConnectorAutoConfiguration.java
│       │   └── EventPublisherProperties.java
│       └── resources/META-INF/spring/
│           └── org.springframework.boot.autoconfigure.AutoConfiguration.imports
└── clickhouse-batch-sink/
    ├── pom.xml
    └── src/main/
        ├── java/io/anasy/sink/
        │   ├── ClickHouseBatchSinkApplication.java
        │   └── ClickHouseSinkListener.java
        └── resources/application.yml
```

### Parent BOM (sketch)

Java 21, Spring Boot 3.4.x. Modules depend on Boot, not on each other.

`event-connector-starter` dependencies: `spring-boot-starter`, `spring-kafka`, `jackson-databind`, `spring-boot-autoconfigure`, `spring-boot-configuration-processor` (optional).

`clickhouse-batch-sink` dependencies: `spring-boot-starter`, `spring-kafka`, `spring-boot-starter-jdbc`, `com.clickhouse:clickhouse-jdbc`.

OLTP services depend only on the starter.

## Setup

### Local brokers

```yaml
# docker-compose.yml (dev only)
services:
  kafka:
    image: apache/kafka:3.9.1
    ports: ["9092:9092"]
  clickhouse:
    image: clickhouse/clickhouse-server:24.12
    ports: ["8123:8123", "9000:9000"]
```

Create the database and table from [schema.sql](schema.sql). Create topics with a partition count you are willing to scale to (see [scaling.md](scaling.md)).

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic orders.events --partitions 12 --replication-factor 1
```

### Install the starter in an OLTP service

```xml
<dependency>
  <groupId>io.anasy</groupId>
  <artifactId>event-connector-starter</artifactId>
  <version>1.0.0</version>
</dependency>
```

No `@Import`. The auto-configuration class is listed in `AutoConfiguration.imports`. `EventPublisher` is injectable when `KafkaTemplate` is on the classpath.

---

## Producer: auto-configuration

Boot 3 loads:

```
# event-connector-starter/.../AutoConfiguration.imports
io.anasy.connector.EventConnectorAutoConfiguration
```

```java
package io.anasy.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

@AutoConfiguration(after = KafkaAutoConfiguration.class)
@ConditionalOnClass(KafkaTemplate.class)
@EnableConfigurationProperties(EventPublisherProperties.class)
@ConditionalOnProperty(prefix = "anas.publisher", name = "enabled", havingValue = "true", matchIfMissing = true)
public class EventConnectorAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(EventPublisher.class)
    public EventPublisher eventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                         ObjectMapper objectMapper,
                                         EventPublisherProperties properties) {
        return new KafkaEventPublisher(kafkaTemplate, objectMapper, properties);
    }
}
```

`@ConditionalOnMissingBean` lets a service replace the publisher in tests.

### Publisher API

```java
package io.anasy.connector;

public interface EventPublisher {

    /** Publishes with a generated eventId as the Kafka key. */
    void publish(String topic, Object payload);

    /** Publishes with a stable eventId. Use this when the caller already has one. */
    void publish(String topic, String eventId, Object payload);
}
```

```java
package io.anasy.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

public final class KafkaEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final EventPublisherProperties properties;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafka,
                               ObjectMapper mapper,
                               EventPublisherProperties properties) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.properties = properties;
    }

    @Override
    public void publish(String topic, Object payload) {
        publish(topic, UUID.randomUUID().toString(), payload);
    }

    @Override
    public void publish(String topic, String eventId, Object payload) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic is required");
        }
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId is required");
        }
        String json;
        try {
            json = payload instanceof String s ? s : mapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("payload is not JSON-serializable", e);
        }
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, eventId, json);
        kafka.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("anas publish failed topic={} eventId={}", topic, eventId, ex);
            } else if (properties.isLogSuccess()) {
                log.debug("anas published topic={} eventId={} offset={}",
                        topic, eventId, result.getRecordMetadata().offset());
            }
        });
    }
}
```

OLTP usage stays one call after the transactional write:

```java
@Service
public class OrderService {

    private final OrderRepository orders;
    private final EventPublisher events;

    public OrderService(OrderRepository orders, EventPublisher events) {
        this.orders = orders;
        this.events = events;
    }

    public Order place(OrderRequest request) {
        Order saved = orders.save(request.toOrder());
        events.publish("orders.events", saved.id().toString(), FatOrderEvent.from(saved));
        return saved;
    }
}
```

Prefer a stable business id as `eventId` when one insert maps to one event (order id). Generate a UUID when the same aggregate emits many events.

Avro: set `spring.kafka.producer.value-serializer` to an Avro serializer and inject `KafkaTemplate<String, ?>`. Do not add an Avro module until a service actually needs it.

### Producer `application.yml`

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
      retries: 3
      properties:
        enable.idempotence: true
        max.in.flight.requests.per.connection: 5

anas:
  publisher:
    enabled: true
    log-success: false
```

`EventPublisherProperties` is a `@ConfigurationProperties(prefix = "anas.publisher")` record with `boolean enabled` and `boolean logSuccess`. Reuse `spring.kafka.*` for brokers and serializers. Do not duplicate bootstrap servers under `anas.*`.

---

## Consumer: batch sink

Spring Boot JDBC auto-configures `DataSource` + `JdbcTemplate` from `spring.datasource.*`. No custom ClickHouse config class.

```java
package io.anasy.sink;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ClickHouseSinkListener {

    private static final String INSERT = """
            INSERT INTO fat_events
              (event_id, topic, kafka_partition, kafka_offset, event_ts, payload)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbc;

    public ClickHouseSinkListener(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @KafkaListener(topics = "${anas.sink.topics}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(List<ConsumerRecord<String, String>> records) {
        if (records.isEmpty()) {
            return;
        }
        jdbc.batchUpdate(INSERT, records, records.size(), (ps, rec) -> {
            ps.setString(1, requireKey(rec));
            ps.setString(2, rec.topic());
            ps.setInt(3, rec.partition());
            ps.setLong(4, rec.offset());
            ps.setTimestamp(5, Timestamp.from(Instant.ofEpochMilli(rec.timestamp())));
            ps.setString(6, rec.value() == null ? "" : rec.value());
        });
    }

    private static String requireKey(ConsumerRecord<String, String> rec) {
        if (rec.key() == null || rec.key().isBlank()) {
            throw new IllegalStateException(
                    "missing eventId key topic=%s partition=%d offset=%d"
                            .formatted(rec.topic(), rec.partition(), rec.offset()));
        }
        return rec.key();
    }
}
```

A missing key is a poison record: fail the batch, let the DLT recoverer take it after retries. Do not invent an id.

`event_date` is `DEFAULT toDate(event_ts)`. `ingested_at` is `DEFAULT now()`. Neither is bound in JDBC.

### Sink `application.yml`

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: anas-clickhouse-sink
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      enable-auto-commit: false
      max-poll-records: 10000
      fetch-min-size: 1MB
      fetch-max-wait: 500ms
      properties:
        max.partition.fetch.bytes: 10485760
    listener:
      type: batch
      ack-mode: batch
      idle-between-polls: 0
    producer: # DLT
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer

  datasource:
    url: jdbc:clickhouse:http://localhost:8123/anas
    driver-class-name: com.clickhouse.jdbc.ClickHouseDriver
    hikari:
      maximum-pool-size: 4
      connection-timeout: 10000

anas:
  sink:
    topics: orders.events,payments.events
```

`max-poll-records: 10000` is the ClickHouse-healthy default. Drop it for large payloads; see [scaling.md](scaling.md).

Hikari pool stays small. The listener is single-threaded per container by default; four connections cover retries, not parallelism. Raise `spring.kafka.listener.concurrency` only up to the topic's partition count.

### DLT (poison batches)

```java
@Bean
public DefaultErrorHandler sinkErrorHandler(KafkaTemplate<String, String> template) {
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
    DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
    handler.setCommitRecovered(true);
    return handler;
}
```

Three retries, then `{topic}.DLT`. Bounded. A bad JSON value must not block a partition indefinitely.

---

## ClickHouse schema

Query patterns, in order: filter by `topic` + date range; extract JSON fields; rare lookup by `event_id`.

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
| `event_id` | `UUID` | Dedup key. From Kafka key, 16 bytes |
| `topic` | `LowCardinality(String)` | Few topics; leading `ORDER BY` column |
| `kafka_partition` / `kafka_offset` | unsigned ints | Debug / replay. Not the dedup key |
| `event_ts` | `DateTime` | Kafka record timestamp |
| `event_date` | `Date` | Coarse key + partition + TTL. Not `Nullable` |
| `payload` | `String` | Raw JSON. Opaque at ingest |
| `ingested_at` | `DateTime` | `ReplacingMergeTree` version; last insert wins |

`ORDER BY (topic, event_date, event_id)` is the replacement key. Same event retried after midnight still shares `event_date` because `event_ts` is the Kafka timestamp of the original record, not `now()`.

TTL 13 months keeps a year of reporting plus a month of overlap. `DROP PARTITION` is the operational delete; do not `ALTER DELETE` the history.

Payload stays `String`, not the ClickHouse `JSON` type. Ingest must not depend on payload shape or on a specific ClickHouse JSON version. Promote hot paths with materialized columns or a typed table when a field is queried constantly.

---

## JSON extraction

Always filter `topic` and `event_date` first so the primary index is used.

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

Nested fields:

```sql
SELECT JSONExtractString(payload, 'customer', 'id') AS customer_id
FROM anas.fat_events
WHERE topic = 'orders.events'
  AND event_date = today()
LIMIT 10;
```

Latest version of a replaced event:

```sql
SELECT event_id, payload
FROM anas.fat_events FINAL
WHERE topic = 'orders.events'
  AND event_date = today()
  AND event_id = '11111111-1111-1111-1111-111111111111';
```

`FINAL` is for point reads and small ranges. For reports, prefer `argMax` or accept that merges eventually collapse duplicates.

```sql
SELECT
    event_id,
    argMax(payload, ingested_at) AS payload
FROM anas.fat_events
WHERE topic = 'orders.events'
  AND event_date >= today() - 1
GROUP BY event_id;
```

Repeated dashboard aggregations belong in an incremental materialized view, not in a full scan of `payload`. Add that view when a query is actually hot — not before.

```sql
-- example only, after the access pattern exists
ALTER TABLE anas.fat_events
    ADD COLUMN amount Float64 DEFAULT JSONExtractFloat(payload, 'totalAmount');
```

Materialized columns fill on insert and on merge; they do not rewrite history until a mutation, which we do not run for this.

---

## Tests that earn their keep

One check per module. No test frameworks beyond JUnit.

- Starter: `KafkaEventPublisher` writes a `ProducerRecord` whose key is the given `eventId` and whose value is JSON. Fail if key is generated when an id was passed.
- Sink: listener binds `event_id` from `record.key()`, not from `UUID.randomUUID()`. A null key throws.

That is enough to lock the idempotency contract.
