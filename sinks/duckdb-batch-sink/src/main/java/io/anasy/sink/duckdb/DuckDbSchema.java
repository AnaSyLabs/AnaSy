package io.anasy.sink.duckdb;

import jakarta.annotation.PostConstruct;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class DuckDbSchema {

    private final JdbcTemplate jdbc;

    public DuckDbSchema(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @PostConstruct
    void createTable() {
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS fat_events (
                  event_id        VARCHAR PRIMARY KEY,
                  topic           VARCHAR NOT NULL,
                  kafka_partition INTEGER,
                  kafka_offset    BIGINT,
                  event_ts        TIMESTAMP,
                  event_date      DATE,
                  payload         VARCHAR,
                  ingested_at     TIMESTAMP DEFAULT current_timestamp
                )
                """);
    }
}
