package com.gkcontas.orders.exception;

public class DeadLetterOperationException extends RuntimeException {

    public DeadLetterOperationException(String message) {
        super(message);
    }

    public DeadLetterOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
