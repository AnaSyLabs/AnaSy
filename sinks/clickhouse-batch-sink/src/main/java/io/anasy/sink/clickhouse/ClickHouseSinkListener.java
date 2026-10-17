package io.anasy.sink.clickhouse;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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

    @KafkaListener(topics = "#{'${anas.sink.topics}'.split(',')}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(List<ConsumerRecord<String, String>> records) {
        List<ConsumerRecord<String, String>> good = new ArrayList<>();
        for (var rec : records) {
            if (!isUuid(rec.key())) {
                dlq.accept(rec, new PoisonEventException(
                        "eventId is not a UUID topic=%s partition=%d offset=%d"
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

    /** event_id is a ClickHouse UUID column; one bad key would otherwise fail and retry the whole batch. */
    private static boolean isUuid(String key) {
        if (key == null) {
            return false;
        }
        try {
            return UUID.fromString(key).toString().equalsIgnoreCase(key);
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
