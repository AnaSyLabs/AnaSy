package io.anasy.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class KafkaEventPublisherTest {

    private final CapturingTemplate kafka = new CapturingTemplate();
    private final KafkaEventPublisher publisher =
            new KafkaEventPublisher(kafka, new ObjectMapper(), new EventPublisherProperties());

    @Test
    void givenEventIdIsKafkaKeyAndValueIsJsonForPublishAndPublishAndWait() {
        kafka.succeed();
        var payload = new Fat("sku-1");

        publisher.publish("orders.events", "evt-1", payload);
        publisher.publishAndWait("orders.events", "evt-1", payload);

        assertEquals(2, kafka.sent.size());
        for (ProducerRecord<String, String> rec : kafka.sent) {
            assertEquals("orders.events", rec.topic());
            assertEquals("evt-1", rec.key());
            assertEquals("{\"sku\":\"sku-1\"}", rec.value());
        }
    }

    @Test
    void publishAndWaitThrowsWhenSendFails() {
        kafka.fail(new RuntimeException("broker down"));
        assertThrows(IllegalStateException.class,
                () -> publisher.publishAndWait("orders.events", "evt-1", "{}"));
    }

    record Fat(String sku) {}

    static final class CapturingTemplate extends KafkaTemplate<String, String> {
        final List<ProducerRecord<String, String>> sent = new ArrayList<>();
        private CompletableFuture<SendResult<String, String>> reply;

        CapturingTemplate() {
            super(new DefaultKafkaProducerFactory<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1",
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)));
        }

        void succeed() {
            var record = new ProducerRecord<>("orders.events", "evt-1", "{}");
            var meta = new RecordMetadata(new TopicPartition("orders.events", 0), 0L, 0, 0L, 0, 0);
            reply = CompletableFuture.completedFuture(new SendResult<>(record, meta));
        }

        void fail(Throwable t) {
            reply = CompletableFuture.failedFuture(t);
        }

        @Override
        public CompletableFuture<SendResult<String, String>> send(ProducerRecord<String, String> record) {
            sent.add(record);
            return reply;
        }
    }
}
