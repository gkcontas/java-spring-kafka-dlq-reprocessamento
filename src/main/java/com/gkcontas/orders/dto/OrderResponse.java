package com.gkcontas.orders.dto;

import com.gkcontas.orders.model.Order;
import java.math.BigDecimal;
import java.time.Instant;

public record OrderResponse(
        Long id,
        String eventId,
        String customerEmail,
        String item,
        Integer quantity,
        BigDecimal totalAmount,
        Instant processedAt) {

    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.getId(),
                order.getEventId(),
                order.getCustomerEmail(),
                order.getItem(),
                order.getQuantity(),
                order.getTotalAmount(),
                order.getProcessedAt());
    }
}
