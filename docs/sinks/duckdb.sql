-- DuckDB sink only. Not an ANASy core schema.
-- See docs/sinks/duckdb.md. PRIMARY KEY is the idempotency key (Kafka eventId).

CREATE TABLE IF NOT EXISTS fat_events (
    event_id        VARCHAR PRIMARY KEY,
    topic           VARCHAR NOT NULL,
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    event_ts        TIMESTAMP,
    event_date      DATE,
    payload         VARCHAR,
    ingested_at     TIMESTAMP DEFAULT current_timestamp
);
