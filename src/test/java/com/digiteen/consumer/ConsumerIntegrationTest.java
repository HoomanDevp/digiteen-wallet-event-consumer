package com.digiteen.consumer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ConsumerIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse(
            "postgres:18.3@sha256:7e32e9833a6fb1c92c32552794cb6ed569d51b445a54907d35fc112ef39684db").asCompatibleSubstituteFor("postgres"))
            .withDatabaseName("consumer").withUsername("consumer").withPassword("test-only");
    @Container
    static final RabbitMQContainer rabbit = new RabbitMQContainer(DockerImageName.parse(
            "rabbitmq:4.2.5-management@sha256:e43ca03df740d48770641b56ce1a26913c74a31e47c1ed36a58e8d91c10731b9").asCompatibleSubstituteFor("rabbitmq"));

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.rabbitmq.host", rabbit::getHost);
        registry.add("spring.rabbitmq.port", rabbit::getAmqpPort);
        registry.add("spring.rabbitmq.username", rabbit::getAdminUsername);
        registry.add("spring.rabbitmq.password", rabbit::getAdminPassword);
    }

    @Autowired ProcessedEventService service;
    @Autowired JdbcTemplate jdbc;
    @Autowired RabbitTemplate template;
    @Autowired RabbitAdmin admin;
    @Autowired RabbitListenerEndpointRegistry listeners;
    @Autowired ObjectMapper mapper;
    private final List<ILoggingEvent> logs = new CopyOnWriteArrayList<>();
    private final List<Boolean> committedRowsVisibleAtLog = new CopyOnWriteArrayList<>();
    private AppenderBase<ILoggingEvent> appender;

    @BeforeEach
    void reset() {
        listeners.stop();
        admin.initialize();
        admin.purgeQueue(RabbitTopology.QUEUE, false);
        jdbc.execute("TRUNCATE processed_events");
        Logger logger = (Logger) LoggerFactory.getLogger(EventListener.class);
        appender = new AppenderBase<>() {
            @Override protected void append(ILoggingEvent event) {
                event.prepareForDeferredProcessing();
                if (event.getFormattedMessage().equals("consumption_committed")) {
                    committedRowsVisibleAtLog.add(count() == 1);
                }
                logs.add(event);
            }
        };
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void cleanup() {
        listeners.stop();
        jdbc.execute("DROP TRIGGER IF EXISTS fail_insert ON processed_events");
        jdbc.execute("DROP FUNCTION IF EXISTS fail_insert()");
        jdbc.execute("DROP SEQUENCE IF EXISTS failed_attempts");
        ((Logger) LoggerFactory.getLogger(EventListener.class)).detachAppender(appender);
        appender.stop();
    }

    @Test
    void concurrentDuplicateTransactionsInsertExactlyOneCommittedRow() throws Exception {
        var event = EventListenerTest.event("TRANSFER", 1L, 2L);
        var start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(8)) {
            var futures = java.util.stream.IntStream.range(0, 8).mapToObj(i -> executor.submit(() -> {
                assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                return service.record(event);
            })).toList();
            start.countDown();
            int inserted = 0;
            for (var future : futures) {
                if (future.get(15, TimeUnit.SECONDS)) inserted++;
            }
            assertThat(inserted).isEqualTo(1);
        }
        assertThat(count()).isEqualTo(1);
        var row = jdbc.queryForMap("SELECT * FROM processed_events WHERE event_id = ?", event.eventId());
        assertThat(row.get("transaction_id")).isEqualTo(event.transactionId());
        assertThat(row.get("type")).isEqualTo("TRANSFER");
        assertThat(row.get("trace_id")).isEqualTo(event.traceId());
        assertThat(row.get("processed_at")).isNotNull();
    }

    @Test
    void realRabbitDuplicateDeliveryCommitsOneAuditAndAcknowledgesBoth() {
        var event = EventListenerTest.event("DEPOSIT", null, 1L);
        publish(event);
        publish(event);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(ready()).isEqualTo(2));
        listeners.start();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(count()).isEqualTo(1);
            assertThat(logs).extracting(ILoggingEvent::getFormattedMessage)
                    .contains("consumption_committed", "consumption_duplicate");
            assertThat(queueMetric("messages")).isZero();
            assertThat(queueMetric("messages_unacknowledged")).isZero();
        });
        var committed = logs.stream().filter(e -> e.getFormattedMessage().equals("consumption_committed"))
                .findFirst().orElseThrow();
        assertThat(committed.getKeyValuePairs()).extracting(kv -> kv.key)
                .contains("eventId", "transactionId", "type");
        assertThat(committed.getMDCPropertyMap()).containsEntry("traceId", event.traceId().toString());
        assertThat(committedRowsVisibleAtLog).containsExactly(true);
    }

    @Test
    void databaseFailureRequeuesWithoutSuccessAckAndRecoversAfterFailureRemoved() {
        jdbc.execute("CREATE SEQUENCE failed_attempts");
        // Sequence advancement survives rollback, giving deterministic evidence of attempted writes.
        jdbc.execute("""
                CREATE FUNCTION fail_insert() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    PERFORM nextval('failed_attempts');
                    PERFORM pg_sleep(0.1);
                    RAISE EXCEPTION 'test database write unavailable' USING ERRCODE = '08006';
                END $$
                """);
        jdbc.execute("CREATE TRIGGER fail_insert BEFORE INSERT ON processed_events FOR EACH ROW EXECUTE FUNCTION fail_insert()");
        var event = EventListenerTest.event("WITHDRAWAL", 1L, null);
        publish(event);
        listeners.start();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(jdbc.queryForObject("SELECT last_value FROM failed_attempts", Long.class))
                    .isGreaterThanOrEqualTo(2L);
            assertThat(logs).extracting(ILoggingEvent::getFormattedMessage).contains("consumption_failed");
        });
        listeners.stop();
        assertThat(count()).isZero();
        assertThat(ready()).isEqualTo(1);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(queueMetric("messages_ready")).isEqualTo(1);
            assertThat(queueMetric("messages_unacknowledged")).isZero();
        });
        assertThat(logs).extracting(ILoggingEvent::getFormattedMessage)
                .doesNotContain("consumption_committed", "consumption_duplicate");
        jdbc.execute("DROP TRIGGER fail_insert ON processed_events");
        listeners.start();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(count()).isEqualTo(1);
            assertThat(logs).extracting(ILoggingEvent::getFormattedMessage).contains("consumption_committed");
            assertThat(queueMetric("messages")).isZero();
        });
    }

    @Test
    void poisonMessageIsRejectedNotRequeuedOrPersisted() {
        template.send(RabbitTopology.EXCHANGE, RabbitTopology.ROUTING_KEY,
                new Message("{malformed-sensitive-payload".getBytes(StandardCharsets.UTF_8), new MessageProperties()));
        listeners.start();
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(logs).extracting(ILoggingEvent::getFormattedMessage).contains("consumption_failed");
            assertThat(queueMetric("messages")).isZero();
        });
        assertThat(count()).isZero();
        assertThat(logs).allSatisfy(e -> {
            assertThat(e.getFormattedMessage()).doesNotContain("sensitive-payload");
            assertThat(e.getThrowableProxy()).isNull();
        });
    }

    private void publish(WalletOperationEvent event) {
        var properties = new MessageProperties();
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        template.send(RabbitTopology.EXCHANGE, RabbitTopology.ROUTING_KEY,
                new Message(mapper.writeValueAsBytes(event), properties));
    }

    private int count() { return jdbc.queryForObject("SELECT count(*) FROM processed_events", Integer.class); }
    private long ready() { return admin.getQueueInfo(RabbitTopology.QUEUE).getMessageCount(); }

    private int queueMetric(String name) throws Exception {
        String credentials = rabbit.getAdminUsername() + ":" + rabbit.getAdminPassword();
        var request = HttpRequest.newBuilder(URI.create("http://" + rabbit.getHost() + ":"
                + rabbit.getHttpPort() + "/api/queues/%2F/" + RabbitTopology.QUEUE))
                .timeout(Duration.ofSeconds(3)).header("Authorization", "Basic "
                        + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8))).GET().build();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()) {
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            var metric = mapper.readTree(response.body()).get(name);
            assertThat(metric).as("Rabbit management queue statistic %s", name).isNotNull();
            return metric.asInt();
        }
    }
}
