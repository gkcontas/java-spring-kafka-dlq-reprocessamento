package com.gkcontas.orders.dto;

import java.time.Instant;

/**
 * @param payload the raw JSON as it sits in the log. Read as text rather than deserialized,
 *                so a record that landed here <em>because</em> it could not be deserialized
 *                is still inspectable — which is exactly when someone needs to see it.
 */
public record DeadLetterRecordResponse(
        int partition,
        long offset,
        String key,
        String payload,
        String originalTopic,
        Integer originalPartition,
        Long originalOffset,
        String exceptionType,
        String exceptionMessage,
        int reprocessAttempts,
        Instant timestamp) {
}
