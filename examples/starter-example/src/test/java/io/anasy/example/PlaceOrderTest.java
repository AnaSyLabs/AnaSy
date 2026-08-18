package io.anasy.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = OrderService.TOPIC)
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
class PlaceOrderTest {

    @Autowired
    private TestRestTemplate http;

    @Autowired
    private EmbeddedKafkaBroker kafka;

    @Test
    void placePublishesEventIdAsKafkaKeyForPublishAndPublishAndWait() {
        try (Consumer<String, String> consumer = consumer()) {
            kafka.consumeFromAnEmbeddedTopic(consumer, OrderService.TOPIC);

            assertPublished(consumer, post("/orders", "Ada", 42.5));
            assertPublished(consumer, post("/orders?wait=true", "Grace", 9.0));
        }
    }

    private Order post(String path, String customer, double amount) {
        ResponseEntity<Order> response =
                http.postForEntity(path, new OrderRequest(customer, amount), Order.class);
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        return response.getBody();
    }

    private static void assertPublished(Consumer<String, String> consumer, Order order) {
        ConsumerRecord<String, String> rec =
                KafkaTestUtils.getSingleRecord(consumer, OrderService.TOPIC, Duration.ofSeconds(10));
        assertEquals(order.id().toString(), rec.key());
        assertTrue(rec.value().contains(order.customerName()));
        assertTrue(rec.value().contains("order.placed"));
    }

    private Consumer<String, String> consumer() {
        Map<String, Object> props = KafkaTestUtils.consumerProps("starter-example-it", "true", kafka);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<String, String>(props).createConsumer();
    }
}
