package com.gkcontas.orders.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.gkcontas.orders.dto.DeadLetterRecordResponse;
import com.gkcontas.orders.event.OrderPlacedEvent;
import com.gkcontas.orders.exception.InventoryUnavailableException;
import com.gkcontas.orders.exception.OrderValidationException;
import com.gkcontas.orders.service.DemoFailureRules;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OrderFlowIntegrationTest extends IntegrationTestBase {

    @Test
    void shouldProcessAnOrdinaryOrderOnTheFirstAttempt() {
        String eventId = publish("teclado", 2);

        awaitOrderPersisted(eventId);

        assertThat(orderRepository.findByEventId(eventId).orElseThrow().getTotalAmount())
                .isEqualByComparingTo("20.00");
        assertThat(findDeadLetter(eventId)).isEmpty();
    }

    @Test
    void shouldSendAValidationFailureStraightToTheDeadLetterQueue() {
        String eventId = publish("mouse", 0);

        DeadLetterRecordResponse deadLetter = awaitDeadLetter(eventId);

        // The root cause, not Spring Kafka's ListenerExecutionFailedException wrapper.
        // Without the custom headers this reads as a framework class name and a message
        // about a listener method signature, which tells an operator nothing.
        assertThat(deadLetter.exceptionType()).isEqualTo(OrderValidationException.class.getName());
        assertThat(deadLetter.exceptionMessage()).contains("must be positive");
        assertThat(deadLetter.reprocessAttempts()).isZero();
        assertThat(orderRepository.existsByEventId(eventId)).isFalse();
    }

    @Test
    void shouldRecoverThroughRetriesWhenTheFailureIsTransient() {
        // Fails twice, succeeds on the third attempt — inside the retry budget, so it
        // never reaches the dead letter queue. This is the case the retries exist for, and
        // sending it to the DLQ on the first failure would turn every downstream blip into
        // manual work.
        String eventId = publish(DemoFailureRules.FLAKY_ITEM, 1);

        awaitOrderPersisted(eventId);

        assertThat(findDeadLetter(eventId)).isEmpty();
    }

    @Test
    void shouldReachTheDeadLetterQueueOnceTheRetriesAreExhausted() {
        String eventId = publish(DemoFailureRules.ALWAYS_FAILING_ITEM, 1);

        DeadLetterRecordResponse deadLetter = awaitDeadLetter(eventId);

        assertThat(deadLetter.exceptionType()).isEqualTo(InventoryUnavailableException.class.getName());
        // The origin coordinates come from the standard DLT headers and are what make it
        // possible to say where in the main topic the record was.
        assertThat(deadLetter.originalTopic()).isEqualTo("orders");
        assertThat(deadLetter.originalPartition()).isNotNull();
        assertThat(deadLetter.originalOffset()).isNotNull();
    }

    @Test
    void shouldNotProcessTheSameEventTwice() {
        String eventId = UUID.randomUUID().toString();
        OrderPlacedEvent event = new OrderPlacedEvent(
                eventId, "buyer@example.com", "monitor", 1, new BigDecimal("900.00"));

        publisher.publish(event);
        awaitOrderPersisted(eventId);
        publisher.publish(event);

        // Kafka delivers at-least-once and the reprocessing endpoint replays on purpose, so
        // a duplicate is the normal case rather than an anomaly.
        await().atMost(TIMEOUT).until(() -> orderRepository.count() > 0);
        assertThat(orderRepository.findAll().stream()
                .filter(order -> eventId.equals(order.getEventId()))
                .count())
                .as("the unique constraint on event_id is the real guard")
                .isEqualTo(1);
    }

    @Test
    void shouldKeepProcessingTheRestOfThePartitionAfterAPoisonRecord() {
        // A record that can never succeed must not block the ones behind it. Publishing
        // both with the same key puts them on the same partition, which is exactly where a
        // stuck record would stall everything.
        String poisonEventId = UUID.randomUUID().toString();
        publisher.publish(new OrderPlacedEvent(
                poisonEventId, "buyer@example.com", "mouse", -1, new BigDecimal("10.00")));

        String goodEventId = publish("cadeira", 1);

        awaitDeadLetter(poisonEventId);
        awaitOrderPersisted(goodEventId);
    }
}
