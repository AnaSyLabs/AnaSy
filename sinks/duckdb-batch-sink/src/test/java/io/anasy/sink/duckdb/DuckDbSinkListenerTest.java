package io.anasy.sink.duckdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

class DuckDbSinkListenerTest {

    private final CapturingJdbc jdbc = new CapturingJdbc();
    private final RecordingDlq dlq = new RecordingDlq();
    private final DuckDbSinkListener listener = new DuckDbSinkListener(jdbc, dlq);

    @Test
    void bindsEventIdFromKafkaKeyAndDlqsPoisonWithoutBlockingBatch() throws Exception {
        ConsumerRecord<String, String> poison =
                new ConsumerRecord<>("orders.events", 0, 1L, (String) null, "{\"ok\":false}");
        ConsumerRecord<String, String> good =
                new ConsumerRecord<>("orders.events", 0, 2L, "evt-1", "{\"ok\":true}");

        listener.consume(List.of(poison, good));

        assertEquals(1, dlq.recovered.size());
        assertEquals(poison, dlq.recovered.getFirst());
        assertInstanceOf(PoisonEventException.class, dlq.exceptions.getFirst());

        assertEquals(1, jdbc.batch.size());
        assertEquals("evt-1", jdbc.batch.iterator().next().key());

        AtomicReference<String> boundId = new AtomicReference<>();
        PreparedStatement ps = (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class<?>[] {PreparedStatement.class},
                (proxy, method, args) -> {
                    if ("setString".equals(method.getName()) && Integer.valueOf(1).equals(args[0])) {
                        boundId.set((String) args[1]);
                    }
                    Class<?> rt = method.getReturnType();
                    if (!rt.isPrimitive()) {
                        return null;
                    }
                    if (rt == boolean.class) {
                        return false;
                    }
                    if (rt == long.class) {
                        return 0L;
                    }
                    if (rt == double.class) {
                        return 0d;
                    }
                    if (rt == float.class) {
                        return 0f;
                    }
                    return 0;
                });
        jdbc.setter.setValues(ps, good);
        assertEquals("evt-1", boundId.get());
    }

    @Test
    void allPoisonSkipsJdbc() {
        var poison = new ConsumerRecord<>("orders.events", 0, 1L, "  ", "{}");

        listener.consume(List.of(poison));

        assertEquals(1, dlq.recovered.size());
        assertTrue(jdbc.batch == null);
    }

    static final class CapturingJdbc extends JdbcTemplate {
        Collection<ConsumerRecord<String, String>> batch;
        ParameterizedPreparedStatementSetter<ConsumerRecord<String, String>> setter;

        @Override
        public <T> int[][] batchUpdate(String sql, Collection<T> batchArgs, int batchSize,
                                       ParameterizedPreparedStatementSetter<T> pss) {
            @SuppressWarnings("unchecked")
            Collection<ConsumerRecord<String, String>> records =
                    (Collection<ConsumerRecord<String, String>>) batchArgs;
            this.batch = records;
            @SuppressWarnings("unchecked")
            ParameterizedPreparedStatementSetter<ConsumerRecord<String, String>> typed =
                    (ParameterizedPreparedStatementSetter<ConsumerRecord<String, String>>) pss;
            this.setter = typed;
            return new int[0][0];
        }
    }

    static final class RecordingDlq extends DeadLetterPublishingRecoverer {
        final List<ConsumerRecord<?, ?>> recovered = new ArrayList<>();
        final List<Exception> exceptions = new ArrayList<>();

        RecordingDlq() {
            super(new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1",
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))));
        }

        @Override
        public void accept(ConsumerRecord<?, ?> record, Exception exception) {
            recovered.add(record);
            exceptions.add(exception);
        }
    }
}
