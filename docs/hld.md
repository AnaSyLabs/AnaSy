# ANASy high-level design

ANASy (Analytics Sync) offloads fat analytical events from OLTP onto Kafka. Reporting systems consume from Kafka. The warehouse is not part of the core.

Decisions: [PLAN.md](PLAN.md). Core implementation: [lld.md](lld.md). One warehouse: [sinks/clickhouse.md](sinks/clickhouse.md).

## Problem

OLTP services that join five tables to serve a dashboard become slow and coupled to reporting. Fat events — the business row plus every join reporting needs — belong on a bus, not in the request path.

ANASy is that pipe: a producer starter and a Kafka contract. It is not a warehouse, not CDC, and not a query layer.

## Containers

```mermaid
flowchart LR
  subgraph oltp [OLTP services]
    Orders
    Payments
  end

  subgraph starter ["event-connector-starter"]
    EP[EventPublisher]
  end

  Kafka[(Kafka)]

  subgraph sinks [Sinks — one consumer group each]
    CH[clickhouse-batch-sink]
    Other[other warehouse…]
  end

  CHDB[(ClickHouse)]
  OtherDB[(Other OLAP)]

  Orders --> EP
  Payments --> EP
  EP -->|key=eventId value=JSON| Kafka
  Kafka --> CH
  Kafka --> Other
  CH --> CHDB
  Other --> OtherDB
```

| Container | Layer | What it does | What it does not do |
|---|---|---|---|
| `event-connector-starter` | Core | Serialize and send | Know destinations |
| Kafka | Core | Buffer, replay, fan-out | Transform payloads |
| `clickhouse-batch-sink` | Sink | Batch-insert into ClickHouse | Speak for other warehouses |
| Next sink | Sink | Same contract, own `group-id` | Share a process or SPI with ClickHouse |

Two sinks on the same topic both receive every event because they use different consumer groups. That is the whole fan-out design.

## Sequence

```mermaid
sequenceDiagram
  participant S as OLTP service
  participant P as EventPublisher
  participant K as Kafka
  participant A as sink A
  participant B as sink B

  S->>S: persist business row
  S->>S: build fat event
  S->>P: publish or publishAndWait(topic, eventId, payload)
  P->>K: ProducerRecord(key=eventId, value=JSON)
  Note over S,P: publish returns after queueing. publishAndWait waits for broker ack.
  par independent consumers
    K->>A: poll batch
    A->>A: durable write
    A->>K: commit offsets (group A)
  and
    K->>B: poll batch
    B->>B: durable write
    B->>K: commit offsets (group B)
  end
```

A sink that throws does not commit. Kafka redelivers to **that group only**. Other sinks are unaffected. Dedup is per sink, keyed by `eventId`.

Transient warehouse failures retry with exponential backoff on that sink’s thread, then go to **that sink’s DLQ**. Poison records skip backoff.

```mermaid
flowchart TD
  Poll[Poll batch] --> Split{Key present?}
  Split -->|no| DLQ[DLQ immediately]
  Split -->|yes| Write[Warehouse write]
  Write -->|ok| Commit[Commit offsets]
  Write -->|transient fail| Backoff[Exponential backoff]
  Backoff -->|attempts left| Write
  Backoff -->|exhausted| DLQ
  DLQ --> Commit
```

## Kafka contract

This is the only schema ANASy owns. Warehouses map it however they like.

| Kafka field | Source | Meaning |
|---|---|---|
| Key | Producer `eventId` | Stable id. Idempotency key for every sink |
| Timestamp | Producer `CreateTime` | Event time |
| Topic | Caller | Stream name (`orders.events`) |
| Partition / offset | Broker | Replay / debug. Not the idempotency key |
| Value | Fat JSON | Opaque to the starter. Opaque to sinks unless they must type it |

No envelope JSON. No ClickHouse column names in the producer. Producer generates `eventId` before `send`. Sinks never call `UUID.randomUUID()`.

## Trust and failure

| Boundary | Rule |
|---|---|
| OLTP → Kafka | `publish`: fire-and-forget, log send failures. `publishAndWait`: block until broker ack, throw on failure. Neither is transactional with the DB write. |
| Kafka → each sink | At-least-once per consumer group. Batch ack only after a successful write. |
| Sink → warehouse | Transient: exponential backoff (1s × 2, cap 60s, 8 attempts), then that sink’s DLQ. |
| Poison records | Missing key / non-retryable → `{topic}.dlq.{sink}` immediately. Good rows in the same batch still write. |
| DLQ replay | Republish to the original topic with the original `eventId`. Warehouse dedup absorbs duplicates. |

No Kafka transactions in v1. Exactly-once across Kafka and a warehouse is not a core goal.

## What lives where

```mermaid
flowchart TB
  subgraph keep [Stay in OLTP]
    TX[Transactional writes]
    API[Request/response]
  end

  subgraph move [Publish once to ANASy]
    FAT[Joined reporting snapshots]
    HIST[High-volume event history]
  end

  subgraph never [Do not put on this pipe]
    CMD[Commands / sagas]
    PII[Secrets, raw credentials]
  end
```

## Scale sketch

Horizontal scale is Kafka partitions. Each sink group scales on its own: instances × concurrency, capped by partition count. Warehouse tuning stays in that sink’s doc.

Details: [scaling.md](scaling.md).

## v1 non-goals

Sink SPI, multi-writer process, Schema Registry, outbox starter, Avro codegen, admin UI.
