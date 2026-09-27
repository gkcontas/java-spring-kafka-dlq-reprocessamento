package com.gkcontas.orders.exception;

/**
 * A rule the payload will never satisfy, no matter how many times it is delivered.
 *
 * <p>Classified as non-retryable, and that classification is the point: retrying a
 * validation failure is pure waste — the same input produces the same answer, three times
 * slower, with backoff in between. It goes straight to the dead letter queue.
 */
public class OrderValidationException extends RuntimeException {

    public OrderValidationException(String message) {
        super(message);
    }
}
