package com.gkcontas.orders.service;

public enum ProcessingOutcome {
    PROCESSED,
    /** Already seen. Not an error — at-least-once delivery makes this the normal case. */
    DUPLICATE
}
