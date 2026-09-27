package com.gkcontas.orders.service;

import com.gkcontas.orders.exception.InventoryUnavailableException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.stereotype.Component;

/**
 * Makes each failure mode reachable from an HTTP call, by item name.
 *
 * <p>These names are a demo affordance, not business logic, and are kept in one clearly
 * labelled place rather than sprinkled through the processor as plausible-looking rules.
 * A real service would fail because a downstream dependency failed; here the failure has
 * to be summonable on demand so the retry, dead letter and reprocessing paths can all be
 * exercised — from a test and from curl.
 *
 * <ul>
 *   <li>{@value #ALWAYS_FAILING_ITEM} — fails every time, exhausting the retries and
 *       landing in the dead letter queue.</li>
 *   <li>{@value #FLAKY_ITEM} — fails {@value #FLAKY_FAILURES_BEFORE_SUCCESS} times per
 *       event and then succeeds, which is what a real transient fault looks like.</li>
 * </ul>
 */
@Component
public class DemoFailureRules {

    public static final String ALWAYS_FAILING_ITEM = "out-of-stock-widget";
    public static final String FLAKY_ITEM = "flaky-widget";
    public static final String RECOVERING_ITEM = "recovering-widget";
    static final int FLAKY_FAILURES_BEFORE_SUCCESS = 2;
    /**
     * One more than the whole initial retry budget, so this item always exhausts the
     * retries and lands in the dead letter queue — and then succeeds when replayed. That
     * is the shape of the outage the reprocessing endpoint exists for: a dependency down
     * for longer than a few hundred milliseconds of backoff, fixed later by a human.
     */
    static final int RECOVERING_FAILURES_BEFORE_SUCCESS = 3;

    /** Attempt counters are per event id, so one flaky event does not affect another. */
    private final Map<String, AtomicInteger> attemptsByEventId = new ConcurrentHashMap<>();

    public void reserveInventory(String eventId, String item) {
        if (ALWAYS_FAILING_ITEM.equals(item)) {
            throw new InventoryUnavailableException(
                    "Inventory for '%s' is unavailable".formatted(item));
        }
        int failuresBeforeSuccess = switch (item) {
            case FLAKY_ITEM -> FLAKY_FAILURES_BEFORE_SUCCESS;
            case RECOVERING_ITEM -> RECOVERING_FAILURES_BEFORE_SUCCESS;
            default -> 0;
        };
        if (failuresBeforeSuccess == 0) {
            return;
        }

        int attempt = attemptsByEventId
                .computeIfAbsent(eventId, key -> new AtomicInteger())
                .incrementAndGet();
        if (attempt <= failuresBeforeSuccess) {
            throw new InventoryUnavailableException(
                    "Inventory service timed out for '%s' (attempt %d)".formatted(item, attempt));
        }
    }
}
