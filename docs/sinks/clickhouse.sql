-- ClickHouse sink only. Not an ANASy core schema.
-- See docs/sinks/clickhouse.md. ORDER BY is immutable.

CREATE DATABASE IF NOT EXISTS anas;

CREATE TABLE IF NOT EXISTS anas.fat_events
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
