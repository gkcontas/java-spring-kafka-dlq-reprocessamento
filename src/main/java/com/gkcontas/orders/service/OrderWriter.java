package com.gkcontas.orders.service;

import com.gkcontas.orders.event.OrderPlacedEvent;
import com.gkcontas.orders.model.Order;
import com.gkcontas.orders.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Its own bean so the {@code @Transactional} boundary actually exists: called from inside
 * {@link OrderService} it would bypass the proxy and the annotation would do nothing.
 */
@Service
public class OrderWriter {

    private final OrderRepository orderRepository;

    public OrderWriter(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Transactional
    public void persist(OrderPlacedEvent event) {
        orderRepository.save(Order.from(event));
    }
}
