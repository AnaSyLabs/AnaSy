package io.anasy.sink.duckdb;

import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/** ponytail: local viewer read-path. Ceiling is a read replica; upgrade if this contends with inserts. */
@RestController
public class DuckDbQueryController {

    private final JdbcTemplate jdbc;

    public DuckDbQueryController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/internal/health")
    public Map<String, Object> health() {
        Integer n = jdbc.queryForObject("SELECT count(*) FROM fat_events", Integer.class);
        return Map.of("ok", true, "rows", n == null ? 0 : n);
    }

    @GetMapping("/internal/events")
    public List<Map<String, Object>> recent() {
        return jdbc.queryForList("""
                SELECT event_id, topic, kafka_partition, kafka_offset, event_ts, payload, ingested_at
                FROM fat_events
                ORDER BY ingested_at DESC
                LIMIT 50
                """);
    }

    @GetMapping("/internal/events/{id}")
    public Map<String, Object> byId(@PathVariable String id) {
        var rows = jdbc.queryForList(
                "SELECT event_id, topic, kafka_partition, kafka_offset, event_ts, payload, ingested_at FROM fat_events WHERE event_id = ?",
                id);
        return rows.isEmpty() ? Map.of() : rows.getFirst();
    }
}
