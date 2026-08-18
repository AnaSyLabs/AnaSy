# Medallion plan

Planning only. Do not build silver or gold until a query is actually hot.

ANASy is the pipe. Medallion is a **ClickHouse modeling** layout on top of that pipe. It does not change the starter, the Kafka contract, or the sink SPI rule.

Decisions that stay locked: [PLAN.md](PLAN.md). Bronze DDL: [sinks/clickhouse.sql](sinks/clickhouse.sql). Query patterns today: [sinks/clickhouse.md](sinks/clickhouse.md).

```
OLTP  →  EventPublisher  →  Kafka          bronze bus (immutable log)
                              │
                              ├─ clickhouse-batch-sink  →  anas.fat_events     BRONZE  (have)
                              └─ duckdb-batch-sink      →  fat_events          BRONZE  (have)

ClickHouse only, later:
  fat_events  →  incremental MV  →  silver typed tables
  silver      →  incremental MV  →  gold rollups
```

## Workload

| | |
|---|---|
| Shape | Product analytics. Fat events already joined in OLTP. |
| Landing | Kafka key = `event_id`, value = opaque JSON, timestamp = event time. |
| Ingest | JDBC `batchUpdate`, 10k–100k rows. Not ClickHouse Kafka engine. Not `async_insert`. |
| Mutability | At-least-once. Latest row wins via `ReplacingMergeTree(ingested_at)`. |
| Queries today | Filter `topic` + `event_date`; `JSONExtract*` on payload; rare lookup by `event_id`. |

## Bronze — have this

Bronze is raw, replayable, and warehouse-local. Kafka is the system of record for replay. Each warehouse’s `fat_events` is that warehouse’s bronze copy.

### Kafka

One topic per stream (`orders.events`). No envelope. No silver topic. Fan-out is a new consumer `group-id`, not a new tier.

### ClickHouse `anas.fat_events`

| Property | Value | Why it is bronze |
|---|---|---|
| `payload` | `String` | Opaque JSON. Ingest does not parse. |
| Engine | `ReplacingMergeTree(ingested_at)` | Redelivery / last write wins. Not a mutation. |
| `ORDER BY` | `(topic, event_date, event_id)` | Matches how we filter. Dedup tuple. |
| `PARTITION BY` | `toYYYYMM(event_date)` | Drop with `DROP PARTITION`. TTL 13 months. |
| Insert | Batch JDBC | Healthy part size. |

Do not change this table’s `ORDER BY`. Do not parse JSON in `event-connector-starter`. Do not type `payload` as ClickHouse `JSON` until field-level filters are the common path and String extract is the bottleneck.

DuckDB `fat_events` is the same tier for that sink: raw payload, PK upsert. Leave it bronze. DuckDB is not the place to grow gold marts.

## Silver — typed, still one row per event

Silver is bronze plus **stable** fields pulled out of `payload`. Still grain = `event_id`. Still ClickHouse-only.

Trigger: a field is filtered or grouped **constantly**, not because a dashboard mock wants columns.

Path (smallest first):

1. **Materialized column on bronze** when one or two fields are hot.

```sql
ALTER TABLE anas.fat_events
    ADD COLUMN amount Float64 DEFAULT JSONExtractFloat(payload, 'totalAmount');
```

2. **Incremental MV → per-topic silver table** when a topic has a stable schema and several typed columns. Inserts into bronze populate silver. Do not add a second Kafka consumer for this.

```sql
-- sketch. Create only when orders.events has a real typed query load.
CREATE TABLE anas.silver_orders
(
    event_id    UUID,
    event_date  Date,
    event_ts    DateTime,
    customer    String,
    amount      Float64,
    ingested_at DateTime
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_id);

CREATE MATERIALIZED VIEW anas.silver_orders_mv TO anas.silver_orders AS
SELECT
    event_id,
    event_date,
    event_ts,
    JSONExtractString(payload, 'customerName') AS customer,
    JSONExtractFloat(payload, 'totalAmount')   AS amount,
    ingested_at
FROM anas.fat_events
WHERE topic = 'orders.events';
```

Rules:

- `event_id` stays the Kafka key. Never mint a UUID in silver.
- Same `event_date` / monthly partition as bronze so TTL/drop stay aligned.
- `ReplacingMergeTree(ingested_at)` again. Late / duplicate Kafka rows replace. No `ALTER UPDATE`.
- Do not `FINAL` in the sink. Readers that need one row use `FINAL` or `argMax`.
- Skip Nullable. Skip a wide table of 100 extracted columns.

Existing bronze rows **do not** backfill through an incremental MV. If history matters, one `INSERT SELECT` into silver after the MV exists, then leave the MV for new inserts.

## Gold — aggregates, not events

Gold is **not** one row per `event_id`. It is a rollup a dashboard actually hits every few seconds.

Trigger: a repeated aggregation that scans bronze/silver enough to hurt. Not “we might want revenue by day later.”

Incremental MV into `AggregatingMergeTree` (or `SummingMergeTree` if the grain is only sums). Refreshable MV only if the transform is a heavy join that cannot run per insert.

```sql
-- sketch. Example grain: orders per day.
CREATE TABLE anas.gold_orders_daily
(
    event_date Date,
    orders     AggregateFunction(count),
    revenue    AggregateFunction(sum, Float64)
)
ENGINE = AggregatingMergeTree()
ORDER BY event_date;

CREATE MATERIALIZED VIEW anas.gold_orders_daily_mv TO anas.gold_orders_daily AS
SELECT
    event_date,
    countState() AS orders,
    sumState(amount) AS revenue
FROM anas.silver_orders
GROUP BY event_date;
```

Query gold with `countMerge` / `sumMerge`. Keep bronze for ad-hoc and for replay. Gold is a cache with a schema, not a second source of truth.

## What we will not do

| Urge | Do this instead |
|---|---|
| Silver Kafka topic | Incremental MV in ClickHouse |
| ClickHouse Kafka table engine | Keep JDBC batch bronze ingest |
| Parse JSON in the starter so “all sinks get columns” | Each warehouse extracts |
| Shared `silver-sink` Java app | Another MV, or a new warehouse `group-id` if the destination is not ClickHouse |
| `OPTIMIZE … FINAL` from a sink | Let merges run |
| Gold before a hot query exists | Stay on bronze + `JSONExtract*` |
| DuckDB gold tables | ClickHouse. DuckDB stays bronze / local debug |

## Build order

| When | What |
|---|---|
| Now | Bronze. Kafka + `fat_events`. Viewer reads bronze. |
| A field is filtered constantly | Materialized column on `fat_events`. |
| A topic has a stable typed query set | Silver table + incremental MV. One backfill `INSERT SELECT`. |
| A dashboard aggregation is hot | Gold rollup + incremental MV. |

No new Maven module for medallion. DDL lives next to the ClickHouse sink (`docs/sinks/` or a later `docs/sinks/clickhouse-silver.sql`) when something ships.

## Checks (when a tier ships)

- Bronze insert still binds `event_id` from `record.key()`.
- Silver row count for a topic ≤ bronze count for that topic (duplicates collapse).
- Gold query does not scan `fat_events`.
- `system.parts` for new tables stays sane (monthly partitions, no tiny inserts).
