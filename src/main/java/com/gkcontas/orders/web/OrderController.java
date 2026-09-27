package com.gkcontas.orders.web;

import com.gkcontas.orders.dto.CreateOrderRequest;
import com.gkcontas.orders.dto.OrderResponse;
import com.gkcontas.orders.event.OrderPlacedEvent;
import com.gkcontas.orders.repository.OrderRepository;
import com.gkcontas.orders.service.DemoFailureRules;
import com.gkcontas.orders.service.OrderPublisher;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderPublisher publisher;
    private final OrderRepository orderRepository;

    public OrderController(OrderPublisher publisher, OrderRepository orderRepository) {
        this.publisher = publisher;
        this.orderRepository = orderRepository;
    }

    /**
     * 202, not 201: this publishes an event. Whether an order comes out the other side
     * depends on a consumer that has not run yet, and claiming "created" here would be a
     * promise the endpoint is in no position to make.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, String> place(@Valid @RequestBody CreateOrderRequest request) {
        String eventId = (request.eventId() == null || request.eventId().isBlank())
                ? UUID.randomUUID().toString()
                : request.eventId();

        publisher.publish(new OrderPlacedEvent(
                eventId, request.customerEmail(), request.item(), request.quantity(), request.unitPrice()));

        return Map.of("eventId", eventId, "status", "PUBLISHED");
    }

    @GetMapping
    public List<OrderResponse> orders() {
        return orderRepository.findAllByOrderByIdAsc().stream().map(OrderResponse::from).toList();
    }

    /** The item names that make each failure path reachable from curl. */
    @GetMapping("/failure-modes")
    public Map<String, String> failureModes() {
        return Map.of(
                "quantity <= 0 or > 100", "OrderValidationException — non-retryable, straight to the DLQ",
                DemoFailureRules.ALWAYS_FAILING_ITEM, "InventoryUnavailableException every time — retries then DLQ",
                DemoFailureRules.FLAKY_ITEM, "fails twice then succeeds — the retries do their job",
                DemoFailureRules.RECOVERING_ITEM, "exhausts the retries, reaches the DLQ, then succeeds on reprocess",
                "anything else", "processed on the first attempt");
    }
}
