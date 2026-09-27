package com.gkcontas.orders.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.orders")
public record OrdersProperties(
        String topic,
        String deadLetterTopic,
        int partitions,
        int maxRetries,
        Duration retryInitialInterval,
        double retryMultiplier,
        int maxReprocessAttempts,
        int maxQuantity) {
}
