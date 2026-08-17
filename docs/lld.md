# ANASy low-level design

Core only: the starter and the Kafka contract. Implements [hld.md](hld.md). Decisions in [PLAN.md](PLAN.md).

ClickHouse is a sink, not this file: [sinks/clickhouse.md](sinks/clickhouse.md).

## Module structure

```
anas/
├── pom.xml
├── event-connector-starter/          # core
│   ├── pom.xml
│   └── src/main/
│       ├── java/io/anasy/connector/
│       │   ├── EventPublisher.java
│       │   ├── KafkaEventPublisher.java
│       │   ├── EventConnectorAutoConfiguration.java
│       │   └── EventPublisherProperties.java
│       └── resources/META-INF/spring/
│           └── org.springframework.boot.autoconfigure.AutoConfiguration.imports
└── sinks/
    └── clickhouse-batch-sink/        # reference warehouse, optional
        ├── pom.xml
        └── src/main/java/io/anasy/sink/clickhouse/
```

Parent `io.anasy:anas`. Modules depend on Boot, not on each other. A new sink is a sibling under `sinks/`. It does not depend on `clickhouse-batch-sink`. Do not add `sink-spi` when the second sink appears — copy the listener loop first; extract a helper only if the third copy hurts.

OLTP services depend only on `event-connector-starter`.

## Setup

### Local Kafka

```yaml
# docker-compose.yml (dev only) — Kafka is required. Warehouses are not.
services:
  kafka:
    image: apache/kafka:3.9.1
    ports: ["9092:9092"]
```

Warehouse containers belong in that sink’s compose overlay. ClickHouse: [sinks/clickhouse.md](sinks/clickhouse.md).

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

No `@Import`. `EventPublisher` is injectable when `KafkaTemplate` is on the classpath.

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

Prefer a stable business id as `eventId` when one insert maps to one event. Generate a UUID when the same aggregate emits many events.

Avro: set `spring.kafka.producer.value-serializer` and inject `KafkaTemplate<String, ?>`. Do not add an Avro module until a service needs it.

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

`EventPublisherProperties` is `@ConfigurationProperties(prefix = "anas.publisher")` with `enabled` and `logSuccess`. Reuse `spring.kafka.*`. Do not put broker lists under `anas.*`.

---

## Sink contract

Every sink is a Spring Boot app with `spring-kafka`. Copy this loop; swap the write. Split poison keys before the write (see Retry and DLQ).

Shared Kafka listener settings (destination-specific numbers live in the sink doc):

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: anas-sink-<warehouse>   # unique per sink
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      enable-auto-commit: false
      max-poll-records: 10000
      properties:
        max.poll.interval.ms: 600000
    listener:
      type: batch
      ack-mode: batch

anas:
  sink:
    name: <warehouse>
    topics: orders.events,payments.events
```

Rules:

| Rule | Detail |
|---|---|
| Unique `group-id` | `anas-sink-clickhouse`, `anas-sink-iceberg`, … Sharing a group splits the topic, it does not fan out |
| Key is `eventId` | Null/blank key is poison. DLQ it. Do not mint a UUID |
| Opaque value | Persist `record.value()` as JSON text unless the warehouse forces typed columns |
| Ack after durable write | Throw on write failure so offsets do not move |
| Dedup in the warehouse | PK / upsert / replace — on `eventId`. Not in a shared Java cache |
| Retry then DLQ | Exponential backoff on transient failures. Per-sink DLQ topic. See below |

### Retry and DLQ

Blocking backoff. Not `@RetryableTopic` (that is per-record and breaks batch inserts).

```yaml
spring:
  kafka:
    consumer:
      properties:
        max.poll.interval.ms: 600000   # > backoff budget + insert time

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
      suffix: .dlq.clickhouse   # orders.events → orders.events.dlq.clickhouse
```

Attempts: 1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s ≈ 3 minutes, then DLQ. `max.poll.interval.ms` must exceed that plus the insert, or Kafka revokes the partition mid-retry.

```java
@Bean
public DeadLetterPublishingRecoverer dlqRecoverer(KafkaTemplate<String, String> template,
                                                 EventSinkProperties props) {
    return new DeadLetterPublishingRecoverer(template, (rec, ex) ->
            new TopicPartition(rec.topic() + props.getDlq().getSuffix(), rec.partition()));
}

@Bean
public DefaultErrorHandler sinkErrorHandler(DeadLetterPublishingRecoverer recoverer,
                                           EventSinkProperties props) {
    var retry = props.getRetry();
    var backOff = new ExponentialBackOffWithMaxRetries(retry.getMaxAttempts() - 1);
    backOff.setInitialInterval(retry.getInitialInterval().toMillis());
    backOff.setMultiplier(retry.getMultiplier());
    backOff.setMaxInterval(retry.getMaxInterval().toMillis());

    var handler = new DefaultErrorHandler(recoverer, backOff);
    handler.setCommitRecovered(true);
    handler.addNotRetryableExceptions(
            PoisonEventException.class,
            DeserializationException.class,
            IllegalArgumentException.class);
    return handler;
}
```

`DeadLetterPublishingRecoverer` keeps the original key and adds `kafka_dlt-*` headers (exception, original topic/offset). Two sinks never share a suffix.

**Poison vs transient.** A blank key will never insert. A ClickHouse timeout might. Split the batch so one poison record does not DLQ the rest:

```java
@KafkaListener(topics = "${anas.sink.topics}", groupId = "${spring.kafka.consumer.group-id}")
public void consume(List<ConsumerRecord<String, String>> records) {
    List<ConsumerRecord<String, String>> good = new ArrayList<>();
    for (var rec : records) {
        if (rec.key() == null || rec.key().isBlank()) {
            dlqRecoverer.accept(rec, new PoisonEventException(
                    "missing eventId topic=%s partition=%d offset=%d"
                            .formatted(rec.topic(), rec.partition(), rec.offset())));
            continue;
        }
        good.add(rec);
    }
    if (!good.isEmpty()) {
        warehouse.write(good); // throw on transient failure → error handler retries the write
    }
}
```

`PoisonEventException` is not retryable. `warehouse.write` throwing `DataAccessException` (or similar) *is* retryable.

**Replay.** Republish DLQ records to the original topic (`kafka_dlt-original-topic` header) with the same key. The sink’s normal path and warehouse dedup do the rest. Do not build a replay app in v1.

Create DLQ topics with the same partition count as the source. Produce with the original partition index.

### Adding a second sink

1. New module `sinks/<name>`.
2. New `group-id`.
3. Batch listener as above.
4. Map Kafka fields to that warehouse. Keep `eventId` as the idempotency key.
5. Document warehouse DDL next to the module, not in this file.

Do not route through ClickHouse. Do not add `if (type == CLICKHOUSE)` in a shared process.

---

## Tests that earn their keep

One check in the starter: `KafkaEventPublisher` writes a `ProducerRecord` whose key is the given `eventId` and whose value is JSON. Fail if a key is generated when an id was passed.

Each sink: write binds `event_id` from `record.key()`. A null key is sent to DLQ and does not prevent the rest of the batch from writing. ClickHouse’s check lives with that module.
