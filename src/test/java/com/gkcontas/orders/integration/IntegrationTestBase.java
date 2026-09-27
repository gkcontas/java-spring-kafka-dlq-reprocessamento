package com.gkcontas.orders.integration;

import static org.awaitility.Awaitility.await;

import com.gkcontas.orders.dto.DeadLetterRecordResponse;
import com.gkcontas.orders.event.OrderPlacedEvent;
import com.gkcontas.orders.repository.OrderRepository;
import com.gkcontas.orders.service.DeadLetterService;
import com.gkcontas.orders.service.OrderPublisher;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared base: one Kafka and one PostgreSQL for the whole JVM.
 *
 * <p>Singleton container pattern, for the usual reason — the {@code @Testcontainers}
 * extension would stop the containers at the end of each test class while Spring keeps the
 * context cached across classes.
 *
 * <p>Because the broker is shared, the topics accumulate records across test classes. Every
 * assertion below therefore keys off a freshly generated event id rather than on counts or
 * on offsets, which would depend on execution order.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@ActiveProfiles("test")
public abstract class IntegrationTestBase {

    protected static final Duration TIMEOUT = Duration.ofSeconds(30);

    @ServiceConnection
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @ServiceConnection
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @Autowired
    protected OrderPublisher publisher;

    @Autowired
    protected OrderRepository orderRepository;

    @Autowired
    protected DeadLetterService deadLetterService;

    protected String publish(String item, int quantity) {
        String eventId = UUID.randomUUID().toString();
        publisher.publish(new OrderPlacedEvent(
                eventId, "buyer@example.com", item, quantity, new BigDecimal("10.00")));
        return eventId;
    }

    protected void awaitOrderPersisted(String eventId) {
        await().atMost(TIMEOUT).until(() -> orderRepository.existsByEventId(eventId));
    }

    protected DeadLetterRecordResponse awaitDeadLetter(String eventId) {
        await().atMost(TIMEOUT).until(() -> findDeadLetter(eventId).isPresent());
        return findDeadLetter(eventId).orElseThrow();
    }

    /** Matches on the event id inside the raw payload, so it is independent of offsets. */
    protected Optional<DeadLetterRecordResponse> findDeadLetter(String eventId) {
        return deadLetterService.list().stream()
                .filter(record -> record.payload() != null && record.payload().contains(eventId))
                .findFirst();
    }
}
