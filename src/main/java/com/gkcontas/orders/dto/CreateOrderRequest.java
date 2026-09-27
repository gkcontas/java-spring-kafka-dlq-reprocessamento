package com.gkcontas.orders.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;

/**
 * @param eventId optional. Supplying it lets a caller send the same event twice on purpose
 *                and watch the deduplication work; left out, one is generated.
 */
public record CreateOrderRequest(
        String eventId,
        @NotBlank(message = "must not be blank") @Email(message = "must be a valid email") String customerEmail,
        @NotBlank(message = "must not be blank") String item,
        @NotNull(message = "must not be null") Integer quantity,
        @NotNull(message = "must not be null") BigDecimal unitPrice) {
}
