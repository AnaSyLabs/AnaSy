package io.anasy.sink.clickhouse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/** Real Kafka and ClickHouse, using the schema in docs/sinks/clickhouse.sql. Skipped without Docker. */
@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(properties = {
        "anas.sink.topics=" + ClickHouseSinkIntegrationTest.TOPIC,
        "anas.sink.retry.initial-interval=500ms",
        "anas.sink.retry.multiplier=1.0",
        "anas.sink.retry.max-attempts=6"})
class ClickHouseSinkIntegrationTest {

    static final String TOPIC = "orders.events";
    static final String DLQ = TOPIC + ".dlq.clickhouse";

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

    @Container
    static final GenericContainer<?> CLICKHOUSE = new GenericContainer<>("clickhouse/clickhouse-server:24.8")
            .withEnv("CLICKHOUSE_PASSWORD", "anas")
            .withCopyFileToContainer(MountableFile.forHostPath("../../docs/sinks/clickhouse.sql"),
                    "/docker-entrypoint-initdb.d/init.sql")
            .withExposedPorts(8123)
            .waitingFor(Wait.forHttp("/ping").forPort(8123));

    @DynamicPropertySource
    static void containers(DynamicPropertyRegistry r) {
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        r.add("spring.datasource.url", () -> "jdbc:clickhouse:http://%s:%d/anas"
                .formatted(CLICKHOUSE.getHost(), CLICKHOUSE.getMappedPort(8123)));
    }

    @Autowired
    JdbcTemplate jdbc;

    @MockitoSpyBean
    ClickHouseSinkListener listener;

    @BeforeAll
    static void topics() throws Exception {
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1), new NewTopic(DLQ, 1, (short) 1))).all().get();
        }
    }

    @Test
    void redeliveredEventIsStoredOnceAndPoisonGoesToTheDlq() {
        String id = UUID.randomUUID().toString();
        send(id, "{\"n\":1}");
        send(id, "{\"n\":1}");
        send("order-42", "{\"bad\":true}");
        String after = UUID.randomUUID().toString();
        send(after, "{\"n\":2}");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(finalCount(id)).isEqualTo(1);
            assertThat(finalCount(after)).isEqualTo(1);
        });
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(dlqKeys(3)).contains("order-42"));
        assertThat(dlqKeys(3)).doesNotContain(id, after);
    }

    @Test
    void shortWarehouseOutageIsRetriedWithoutLosingOrDeadLetteringData() {
        String id = UUID.randomUUID().toString();
        jdbc.execute("RENAME TABLE anas.fat_events TO anas.fat_events_off");
        clearInvocations(listener);
        send(id, "{}");
        verify(listener, timeout(15_000).atLeast(2)).consume(any());
        jdbc.execute("RENAME TABLE anas.fat_events_off TO anas.fat_events");

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(finalCount(id)).isEqualTo(1));
        assertThat(dlqKeys(3)).doesNotContain(id);
    }

    @Test
    void outageLongerThanTheRetriesDeadLettersTheBatchAndItCanBeReplayed() {
        String id = UUID.randomUUID().toString();
        jdbc.execute("RENAME TABLE anas.fat_events TO anas.fat_events_off");
        clearInvocations(listener);
        send(id, "{\"keep\":true}");

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(dlqKeys(3)).contains(id));
        verify(listener, times(6)).consume(any());
        jdbc.execute("RENAME TABLE anas.fat_events_off TO anas.fat_events");
        assertThat(finalCount(id)).isZero();

        send(id, "{\"keep\":true}");
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(finalCount(id)).isEqualTo(1));
    }

    private int finalCount(String id) {
        return jdbc.queryForObject(
                "SELECT count() FROM fat_events FINAL WHERE event_id = toUUIDOrZero(?)", Integer.class, id);
    }

    private static void send(String key, String value) {
        try (var producer = new KafkaProducer<String, String>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class))) {
            producer.send(new ProducerRecord<>(TOPIC, key, value));
            producer.flush();
        }
    }

    /** Keys on the DLQ topic, polling for {@code seconds}. */
    private static List<String> dlqKeys(int seconds) {
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of(DLQ));
            var keys = new java.util.ArrayList<String>();
            long end = System.currentTimeMillis() + seconds * 1000L;
            while (System.currentTimeMillis() < end) {
                consumer.poll(Duration.ofMillis(500)).forEach(r -> keys.add(r.key()));
            }
            return keys;
        }
    }
}
