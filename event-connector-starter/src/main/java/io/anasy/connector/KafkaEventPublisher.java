package io.anasy.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

public final class KafkaEventPublisher implements EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final EventPublisherProperties properties;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafka,
                               ObjectMapper mapper,
                               EventPublisherProperties properties) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.properties = properties;
    }

    @Override
    public void publish(String topic, Object payload) {
        publish(topic, UUID.randomUUID().toString(), payload);
    }

    @Override
    public void publish(String topic, String eventId, Object payload) {
        send(topic, eventId, payload).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("anas publish failed topic={} eventId={}", topic, eventId, ex);
            } else if (properties.isLogSuccess()) {
                log.debug("anas published topic={} eventId={} offset={}",
                        topic, eventId, result.getRecordMetadata().offset());
            }
        });
    }

    @Override
    public void publishAndWait(String topic, Object payload) {
        publishAndWait(topic, UUID.randomUUID().toString(), payload);
    }

    @Override
    public void publishAndWait(String topic, String eventId, Object payload) {
        try {
            SendResult<String, String> result = send(topic, eventId, payload).get();
            if (properties.isLogSuccess()) {
                log.debug("anas published topic={} eventId={} offset={}",
                        topic, eventId, result.getRecordMetadata().offset());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "anas publish interrupted topic=%s eventId=%s".formatted(topic, eventId), e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                    "anas publish failed topic=%s eventId=%s".formatted(topic, eventId), e.getCause());
        }
    }

    private CompletableFuture<SendResult<String, String>> send(String topic, String eventId, Object payload) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic is required");
        }
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId is required");
        }
        String json;
        try {
            json = payload instanceof String s ? s : mapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("payload is not JSON-serializable", e);
        }
        return kafka.send(new ProducerRecord<>(topic, eventId, json));
    }
}
