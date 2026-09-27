package com.gkcontas.orders.exception;

/**
 * A transient failure: the dependency is down, timing out, or momentarily out of stock.
 *
 * <p>Retryable, because the next attempt genuinely may succeed. Sending this to the dead
 * letter queue on the first failure would turn every blip in a downstream service into a
 * pile of records for a human to replay by hand.
 */
public class InventoryUnavailableException extends RuntimeException {

    public InventoryUnavailableException(String message) {
        super(message);
    }
}
