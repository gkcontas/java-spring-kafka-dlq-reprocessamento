package com.gkcontas.orders.unit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.gkcontas.orders.config.OrdersProperties;
import com.gkcontas.orders.event.OrderPlacedEvent;
import com.gkcontas.orders.exception.InventoryUnavailableException;
import com.gkcontas.orders.exception.OrderValidationException;
import com.gkcontas.orders.service.DemoFailureRules;
import com.gkcontas.orders.service.OrderProcessor;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OrderProcessorTest {

    private static final OrdersProperties PROPERTIES = new OrdersProperties(
            "orders", "orders.DLT", 3, 2, Duration.ofMillis(50), 2.0, 3, 100);

    private final OrderProcessor processor = new OrderProcessor(PROPERTIES, new DemoFailureRules());

    @Test
    void shouldRejectANonPositiveQuantityAsNonRetryable() {
        // OrderValidationException is the class registered as non-retryable, so the type
        // thrown here is what decides whether the record burns three attempts.
        assertThatThrownBy(() -> processor.validateAndReserve(event("teclado", 0)))
                .isInstanceOf(OrderValidationException.class)
                .hasMessageContaining("must be positive");
    }

    @Test
    void shouldRejectAQuantityAboveTheConfiguredMaximum() {
        assertThatThrownBy(() -> processor.validateAndReserve(event("teclado", 101)))
                .isInstanceOf(OrderValidationException.class)
                .hasMessageContaining("exceeds the maximum");
    }

    @Test
    void shouldValidateBeforeTouchingTheDownstreamDependency() {
        // The always-failing item would raise InventoryUnavailableException if inventory
        // were consulted. Getting a validation error instead proves validation runs first
        // — which is what keeps a malformed event from costing three retries of a call it
        // was never going to complete.
        assertThatThrownBy(() -> processor.validateAndReserve(
                event(DemoFailureRules.ALWAYS_FAILING_ITEM, -5)))
                .isInstanceOf(OrderValidationException.class);
    }

    @Test
    void shouldRaiseARetryableFailureWhenInventoryIsUnavailable() {
        assertThatThrownBy(() -> processor.validateAndReserve(
                event(DemoFailureRules.ALWAYS_FAILING_ITEM, 1)))
                .isInstanceOf(InventoryUnavailableException.class);
    }

    @Test
    void shouldSucceedOnceTheFlakyDependencyRecovers() {
        OrderPlacedEvent event = event(DemoFailureRules.FLAKY_ITEM, 1);

        assertThatThrownBy(() -> processor.validateAndReserve(event))
                .isInstanceOf(InventoryUnavailableException.class);
        assertThatThrownBy(() -> processor.validateAndReserve(event))
                .isInstanceOf(InventoryUnavailableException.class);
        // Third attempt on the same event id: the counter is per event, so this is the one
        // that succeeds.
        assertThatCode(() -> processor.validateAndReserve(event)).doesNotThrowAnyException();
    }

    @Test
    void shouldCountFailuresPerEventRatherThanGlobally() {
        OrderPlacedEvent first = event(DemoFailureRules.FLAKY_ITEM, 1);
        processorFailsOnce(first);
        processorFailsOnce(first);

        // A different event starts from zero; one flaky event must not consume another's
        // budget.
        assertThatThrownBy(() -> processor.validateAndReserve(event(DemoFailureRules.FLAKY_ITEM, 1)))
                .isInstanceOf(InventoryUnavailableException.class);
    }

    @Test
    void shouldAcceptAnOrdinaryOrder() {
        assertThatCode(() -> processor.validateAndReserve(event("teclado", 2)))
                .doesNotThrowAnyException();
    }

    private void processorFailsOnce(OrderPlacedEvent event) {
        assertThatThrownBy(() -> processor.validateAndReserve(event))
                .isInstanceOf(InventoryUnavailableException.class);
    }

    private static OrderPlacedEvent event(String item, int quantity) {
        return new OrderPlacedEvent(UUID.randomUUID().toString(), "someone@example.com",
                item, quantity, new BigDecimal("10.00"));
    }
}
