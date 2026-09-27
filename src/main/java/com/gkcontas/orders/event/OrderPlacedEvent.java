package com.gkcontas.orders.event;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;

/**
 * @param eventId the deduplication key. Supplied by the producer rather than generated on
 *                arrival: a value created by the consumer would be different on every
 *                delivery of the same event, which is precisely what it must not be.
 */
public record OrderPlacedEvent(
        @NotBlank(message = "must not be blank") String eventId,
        @NotBlank(message = "must not be blank") @Email(message = "must be a valid email") String customerEmail,
        @NotBlank(message = "must not be blank") String item,
        @NotNull(message = "must not be null") Integer quantity,
        @NotNull(message = "must not be null") @Positive(message = "must be positive") BigDecimal unitPrice) {

    public BigDecimal totalAmount() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}
