package com.gkcontas.orders.consumer;

import com.gkcontas.orders.event.OrderPlacedEvent;
import com.gkcontas.orders.service.OrderService;
import com.gkcontas.orders.service.ProcessingOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final OrderService orderService;

    public OrderConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * Deliberately thin. Everything that can fail lives in the service, and every failure
     * is allowed to propagate: swallowing an exception here would acknowledge a record
     * that was never processed, which is data loss dressed up as success.
     *
     * <p>The acknowledgement only happens on the line after the work completed. When the
     * listener throws, the error handler takes over — retrying, or publishing to the dead
     * letter topic — and the container commits the offset only once that recovery is done.
     */
    @KafkaListener(topics = "${app.orders.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderPlaced(OrderPlacedEvent event, Acknowledgment acknowledgment) {
        ProcessingOutcome outcome = orderService.process(event);
        acknowledgment.acknowledge();
        log.debug("Event {} handled with outcome {}", event.eventId(), outcome);
    }
}
