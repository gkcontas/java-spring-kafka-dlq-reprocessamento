package com.gkcontas.orders.config;

import com.gkcontas.orders.exception.OrderValidationException;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaConfig {

    /** The exception that actually caused the failure, not Spring Kafka's wrapper. */
    public static final String FAILURE_TYPE_HEADER = "x-failure-type";
    public static final String FAILURE_MESSAGE_HEADER = "x-failure-message";

    @Bean
    public NewTopic ordersTopic(OrdersProperties properties) {
        return TopicBuilder.name(properties.topic())
                .partitions(properties.partitions())
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic deadLetterTopic(OrdersProperties properties) {
        return TopicBuilder.name(properties.deadLetterTopic())
                .partitions(properties.partitions())
                .replicas(1)
                .build();
    }

    /**
     * The whole failure policy, in one bean.
     *
     * <p>Without an error handler that eventually gives up, a record the listener cannot
     * process is retried forever — and because Kafka delivers a partition in order, that
     * one record blocks <b>every</b> record behind it on the same partition. The poison
     * pill is the reason the dead letter queue exists: somewhere to put the record so the
     * partition can move on.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate,
                                            OrdersProperties properties) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                // Partition -1 means "let the producer decide". The default sends a dead
                // letter to the same partition number it came from, which fails outright
                // if the DLT has fewer partitions than the source topic.
                (record, exception) -> new TopicPartition(properties.deadLetterTopic(), -1));

        // The standard DLT headers record the exception Spring Kafka caught, which is
        // always its own ListenerExecutionFailedException wrapper — so a dead letter
        // arrives labelled with the framework's class name and a message about a listener
        // method signature. Useless for deciding what to do with the record. These two
        // extra headers carry the root cause instead, which is what someone reading the
        // queue actually needs to see.
        recoverer.setHeadersFunction((record, exception) -> {
            Throwable rootCause = rootCauseOf(exception);
            Headers headers = new RecordHeaders();
            headers.add(FAILURE_TYPE_HEADER,
                    rootCause.getClass().getName().getBytes(StandardCharsets.UTF_8));
            headers.add(FAILURE_MESSAGE_HEADER,
                    String.valueOf(rootCause.getMessage()).getBytes(StandardCharsets.UTF_8));
            return headers;
        });

        ExponentialBackOffWithMaxRetries backOff =
                new ExponentialBackOffWithMaxRetries(properties.maxRetries());
        backOff.setInitialInterval(properties.retryInitialInterval().toMillis());
        backOff.setMultiplier(properties.retryMultiplier());

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // The classification that makes the retry budget worth having. A validation
        // failure is the same answer every time, so retrying it only spends the backoff
        // and delays the partition; it goes to the dead letter queue on the first attempt.
        // Everything else — timeouts, a downstream service that is briefly down — gets the
        // retries, because the next attempt genuinely may succeed.
        errorHandler.addNotRetryableExceptions(OrderValidationException.class);

        return errorHandler;
    }

    /**
     * Unwraps to the deepest cause, guarding against a self-referencing chain — a cycle
     * here would hang the recoverer, which runs while the partition is already stalled.
     */
    private static Throwable rootCauseOf(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
