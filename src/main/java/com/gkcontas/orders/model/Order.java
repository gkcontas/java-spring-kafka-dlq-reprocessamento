package com.gkcontas.orders.model;

import com.gkcontas.orders.event.OrderPlacedEvent;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "orders")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false, unique = true, length = 64)
    private String eventId;

    @Column(name = "customer_email", nullable = false, length = 200)
    private String customerEmail;

    @Column(nullable = false, length = 120)
    private String item;

    @Column(nullable = false)
    private Integer quantity;

    @Column(name = "unit_price", nullable = false, precision = 12, scale = 2)
    private BigDecimal unitPrice;

    @Column(name = "total_amount", nullable = false, precision = 12, scale = 2)
    private BigDecimal totalAmount;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    public static Order from(OrderPlacedEvent event) {
        Order order = new Order();
        order.eventId = event.eventId();
        order.customerEmail = event.customerEmail();
        order.item = event.item();
        order.quantity = event.quantity();
        order.unitPrice = event.unitPrice();
        order.totalAmount = event.totalAmount();
        order.processedAt = Instant.now();
        return order;
    }
}
