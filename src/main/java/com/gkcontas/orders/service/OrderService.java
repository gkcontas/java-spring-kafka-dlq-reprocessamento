package com.gkcontas.orders.service;

import com.gkcontas.orders.event.OrderPlacedEvent;
import com.gkcontas.orders.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OrderProcessor orderProcessor;
    private final OrderWriter orderWriter;

    public OrderService(OrderRepository orderRepository, OrderProcessor orderProcessor, OrderWriter orderWriter) {
        this.orderRepository = orderRepository;
        this.orderProcessor = orderProcessor;
        this.orderWriter = orderWriter;
    }

    /**
     * Idempotent by two mechanisms, and both are needed.
     *
     * <p>The existence check handles the ordinary case cheaply. The unique constraint
     * handles the case the check cannot: two deliveries of the same event in flight at
     * once, both passing the check before either has written. Relying on the check alone
     * is a race; relying on the constraint alone means every duplicate costs an exception.
     */
    public ProcessingOutcome process(OrderPlacedEvent event) {
        if (orderRepository.existsByEventId(event.eventId())) {
            log.info("Event {} already processed, skipping", event.eventId());
            return ProcessingOutcome.DUPLICATE;
        }

        orderProcessor.validateAndReserve(event);

        try {
            orderWriter.persist(event);
            return ProcessingOutcome.PROCESSED;
        } catch (DataIntegrityViolationException e) {
            // Lost the race with a concurrent delivery. Reported as a duplicate rather
            // than rethrown: letting this reach the error handler would send a
            // successfully-processed event to the dead letter queue.
            log.info("Event {} was written concurrently, treating as duplicate", event.eventId());
            return ProcessingOutcome.DUPLICATE;
        }
    }
}
