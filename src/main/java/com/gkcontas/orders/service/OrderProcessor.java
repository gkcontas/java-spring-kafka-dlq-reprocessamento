package com.gkcontas.orders.service;

import com.gkcontas.orders.config.OrdersProperties;
import com.gkcontas.orders.event.OrderPlacedEvent;
import com.gkcontas.orders.exception.OrderValidationException;
import org.springframework.stereotype.Service;

@Service
public class OrderProcessor {

    private final OrdersProperties properties;
    private final DemoFailureRules failureRules;

    public OrderProcessor(OrdersProperties properties, DemoFailureRules failureRules) {
        this.properties = properties;
        this.failureRules = failureRules;
    }

    /**
     * Validation first, then the side effect. The order matters: validation throws a
     * non-retryable exception, so getting it out of the way means a malformed event never
     * costs three attempts at a downstream call it was never going to complete.
     */
    public void validateAndReserve(OrderPlacedEvent event) {
        validate(event);
        failureRules.reserveInventory(event.eventId(), event.item());
    }

    private void validate(OrderPlacedEvent event) {
        if (event.quantity() == null || event.quantity() <= 0) {
            throw new OrderValidationException(
                    "Quantity must be positive, got %s".formatted(event.quantity()));
        }
        if (event.quantity() > properties.maxQuantity()) {
            throw new OrderValidationException(
                    "Quantity %d exceeds the maximum of %d".formatted(event.quantity(), properties.maxQuantity()));
        }
        if (event.customerEmail() == null || !event.customerEmail().contains("@")) {
            throw new OrderValidationException(
                    "Customer email is not valid: '%s'".formatted(event.customerEmail()));
        }
    }
}
