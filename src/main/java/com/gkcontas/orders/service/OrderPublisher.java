package com.gkcontas.orders.service;

import com.gkcontas.orders.config.OrdersProperties;
import com.gkcontas.orders.event.OrderPlacedEvent;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderPublisher {

    /**
     * How many times this record has already been pushed back through the main topic.
     * Travels in a header because it has to survive the round trip through the dead letter
     * topic — there is nowhere else to keep it that the next delivery can see.
     */
    public static final String REPROCESS_ATTEMPTS_HEADER = "x-reprocess-attempts";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final OrdersProperties properties;

    public OrderPublisher(KafkaTemplate<String, Object> kafkaTemplate, OrdersProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    public void publish(OrderPlacedEvent event) {
        // The event id is the message key, so every delivery about one order lands on the
        // same partition and is therefore ordered relative to the others.
        kafkaTemplate.send(properties.topic(), event.eventId(), event);
    }

    public void republish(OrderPlacedEvent event, int reprocessAttempts) {
        ProducerRecord<String, Object> record =
                new ProducerRecord<>(properties.topic(), null, event.eventId(), event);
        record.headers().add(REPROCESS_ATTEMPTS_HEADER,
                String.valueOf(reprocessAttempts).getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(record);
    }
}
