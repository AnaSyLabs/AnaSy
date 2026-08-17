<p align="center">
  <img src="assets/logo.png" alt="ANASy Labs — Data · Sync · Scale" width="400" />
</p>

# ANASy

Analytics Sync. Keep OLTP thin: publish fat events once to Kafka. Any number of sinks consume them.

```
OLTP  --EventPublisher-->  Kafka  --group A-->  ClickHouse
                           └────  --group B-->  other warehouse
```

Kafka is the product boundary. ClickHouse is a reference sink, not the architecture.

## Modules

| Module | Layer |
|---|---|
| `event-connector-starter` | Core. Inject `EventPublisher`. |
| `sinks/clickhouse-batch-sink` | Optional. Batch JDBC into ClickHouse. |

Java 21, Spring Boot 3.4, Maven. Group `io.anasy`.

## Docs

| Doc | Read when |
|---|---|
| [docs/PLAN.md](docs/PLAN.md) | Locked decisions |
| [docs/hld.md](docs/hld.md) | Architecture, fan-out |
| [docs/lld.md](docs/lld.md) | Starter, publisher, how to add a sink |
| [docs/scaling.md](docs/scaling.md) | Partitions, groups, per-sink tuning |
| [docs/sinks/clickhouse.md](docs/sinks/clickhouse.md) | ClickHouse config, schema, queries |
| [AGENTS.md](AGENTS.md) | Agent rules |

## Quick path

1. Run Kafka. Depend on `event-connector-starter`.
2. Publish:

```java
events.publish("orders.events", order.id().toString(), fatEvent);
```

3. Add a sink: new app, unique `group-id`, write `record.key()` as `eventId`. ClickHouse: apply `docs/sinks/clickhouse.sql` and run `clickhouse-batch-sink`.
