package com.gkcontas.orders.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gkcontas.orders.config.KafkaConfig;
import com.gkcontas.orders.config.OrdersProperties;
import com.gkcontas.orders.dto.DeadLetterRecordResponse;
import com.gkcontas.orders.dto.RecordCoordinates;
import com.gkcontas.orders.dto.ReprocessResultResponse;
import com.gkcontas.orders.event.OrderPlacedEvent;
import com.gkcontas.orders.exception.DeadLetterOperationException;
import com.gkcontas.orders.model.Disposition;
import com.gkcontas.orders.model.HandledDeadLetter;
import com.gkcontas.orders.repository.HandledDeadLetterRepository;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterService.class);
    /** Safety valve only: the loop normally stops on the record count, not on this. */
    private static final int MAX_EMPTY_POLLS = 3;

    private final ConsumerFactory<?, ?> consumerFactory;
    private final OrdersProperties properties;
    private final OrderPublisher publisher;
    private final HandledDeadLetterRepository handledRepository;
    private final ObjectMapper objectMapper;

    public DeadLetterService(ConsumerFactory<?, ?> consumerFactory,
                             OrdersProperties properties,
                             OrderPublisher publisher,
                             HandledDeadLetterRepository handledRepository,
                             ObjectMapper objectMapper) {
        this.consumerFactory = consumerFactory;
        this.properties = properties;
        this.publisher = publisher;
        this.handledRepository = handledRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Lists what is in the dead letter topic <b>without consuming it</b>.
     *
     * <p>Three things make that true, and all three are necessary:
     * {@code assign} instead of {@code subscribe}, so this consumer joins no group and
     * triggers no rebalance of the real one; auto-commit off and no manual commit, so no
     * offset is ever recorded; and a throwaway random group id, so even an accidental
     * commit could not move the processing group's position.
     *
     * <p>Getting this wrong is how an operator "looking at the DLQ" quietly marks every
     * record in it as consumed.
     */
    public List<DeadLetterRecordResponse> list() {
        Set<RecordCoordinates> alreadyHandled = handledCoordinates();

        try (Consumer<String, String> consumer = createInspectionConsumer()) {
            List<TopicPartition> partitions = partitionsOf(consumer);
            if (partitions.isEmpty()) {
                return List.of();
            }
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);

            // How many records the log holds right now, computed from the watermarks
            // rather than discovered by polling until nothing comes back.
            //
            // The obvious loop — poll a fixed number of times, or poll until
            // consumer.position() catches up with the end offset — costs the full number
            // of rounds on every call even when the topic is empty, because an empty poll
            // is indistinguishable from "not ready yet". With the count known up front,
            // an empty queue returns immediately and a full one stops on the last record.
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            long expectedRecords = partitions.stream()
                    .mapToLong(partition -> endOffsets.getOrDefault(partition, 0L)
                            - beginningOffsets.getOrDefault(partition, 0L))
                    .sum();

            List<DeadLetterRecordResponse> records = new ArrayList<>();
            long collected = 0;
            int consecutiveEmptyPolls = 0;

            while (collected < expectedRecords && consecutiveEmptyPolls < MAX_EMPTY_POLLS) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(300));
                if (polled.isEmpty()) {
                    consecutiveEmptyPolls++;
                    continue;
                }
                consecutiveEmptyPolls = 0;
                for (ConsumerRecord<String, String> record : polled) {
                    collected++;
                    RecordCoordinates coordinates = new RecordCoordinates(record.partition(), record.offset());
                    if (!alreadyHandled.contains(coordinates)) {
                        records.add(toResponse(record));
                    }
                }
            }
            records.sort(Comparator.comparingInt(DeadLetterRecordResponse::partition)
                    .thenComparingLong(DeadLetterRecordResponse::offset));
            return records;
        }
    }

    /**
     * Republishes dead letters to the main topic, or gives up on them.
     *
     * <p>The attempt ceiling is the load-bearing part. Without it a record that fails for a
     * reason replaying cannot fix — a payload referencing a customer that no longer exists,
     * say — circles between the main topic and the dead letter queue for as long as anyone
     * keeps pressing the button, generating a fresh dead letter every lap.
     */
    public ReprocessResultResponse reprocess(List<RecordCoordinates> selection) {
        List<DeadLetterRecordResponse> outstanding = list();
        List<DeadLetterRecordResponse> targets = (selection == null || selection.isEmpty())
                ? outstanding
                : outstanding.stream()
                        .filter(record -> selection.contains(new RecordCoordinates(record.partition(), record.offset())))
                        .toList();

        int republished = 0;
        int discarded = 0;
        List<String> notes = new ArrayList<>();

        for (DeadLetterRecordResponse record : targets) {
            RecordCoordinates coordinates = new RecordCoordinates(record.partition(), record.offset());

            if (record.reprocessAttempts() >= properties.maxReprocessAttempts()) {
                markHandled(coordinates, Disposition.DISCARDED,
                        "reached the reprocessing ceiling of %d".formatted(properties.maxReprocessAttempts()));
                notes.add("%d/%d discarded: reprocessed %d time(s), reaching the ceiling of %d"
                        .formatted(record.partition(), record.offset(),
                                record.reprocessAttempts(), properties.maxReprocessAttempts()));
                discarded++;
                continue;
            }

            Optional<OrderPlacedEvent> event = parse(record.payload());
            if (event.isEmpty()) {
                // A record that failed deserialization on the way in cannot be replayed
                // as an object. Saying so is more useful than a stack trace.
                markHandled(coordinates, Disposition.DISCARDED, "payload cannot be deserialized");
                notes.add("%d/%d discarded: payload is not a valid OrderPlacedEvent"
                        .formatted(record.partition(), record.offset()));
                discarded++;
                continue;
            }

            publisher.republish(event.get(), record.reprocessAttempts() + 1);
            markHandled(coordinates, Disposition.REPROCESSED, null);
            notes.add("%d/%d republished (attempt %d)"
                    .formatted(record.partition(), record.offset(), record.reprocessAttempts() + 1));
            republished++;
        }

        log.info("Reprocessing finished: {} republished, {} discarded", republished, discarded);
        return new ReprocessResultResponse(republished, discarded, notes);
    }

    public void discard(int partition, long offset, String reason) {
        RecordCoordinates coordinates = new RecordCoordinates(partition, offset);
        boolean stillOutstanding = list().stream()
                .anyMatch(record -> record.partition() == partition && record.offset() == offset);
        if (!stillOutstanding) {
            throw new DeadLetterOperationException(
                    "No outstanding dead letter at partition %d offset %d".formatted(partition, offset));
        }
        markHandled(coordinates, Disposition.DISCARDED, reason);
    }

    @Transactional
    void markHandled(RecordCoordinates coordinates, Disposition disposition, String reason) {
        if (handledRepository.existsByTopicAndPartitionIdAndRecordOffset(
                properties.deadLetterTopic(), coordinates.partition(), coordinates.offset())) {
            return;
        }
        handledRepository.save(new HandledDeadLetter(properties.deadLetterTopic(),
                coordinates.partition(), coordinates.offset(), disposition, reason));
    }

    private Set<RecordCoordinates> handledCoordinates() {
        Set<RecordCoordinates> handled = new HashSet<>();
        handledRepository.findAllByTopic(properties.deadLetterTopic())
                .forEach(entry -> handled.add(
                        new RecordCoordinates(entry.getPartitionId(), entry.getRecordOffset())));
        return handled;
    }

    private Optional<OrderPlacedEvent> parse(String payload) {
        try {
            return Optional.ofNullable(objectMapper.readValue(payload, OrderPlacedEvent.class));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private Consumer<String, String> createInspectionConsumer() {
        // Built from the auto-configured ConsumerFactory rather than from KafkaProperties.
        //
        // Not a style preference: KafkaProperties only knows what the configuration files
        // say. Anything that supplies the broker address through a bean instead —
        // Testcontainers' @ServiceConnection being the obvious case, but also any custom
        // ConnectionDetails — never touches those properties, so a client assembled from
        // them points at whatever the yml happened to name and blocks until the API
        // timeout expires. The factory's own configuration is the one the application's
        // real consumers use, which is by definition the correct one.
        Map<String, Object> config = new HashMap<>(consumerFactory.getConfigurationProperties());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "dead-letter-inspector-" + UUID.randomUUID());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Plain strings: the point is to see the bytes that are actually in the log, not
        // to re-run the deserialization that may have put them there.
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.keySet().removeIf(key -> key.startsWith("spring."));
        return new DefaultKafkaConsumerFactory<String, String>(config).createConsumer();
    }

    private List<TopicPartition> partitionsOf(Consumer<String, String> consumer) {
        List<PartitionInfo> info = consumer.partitionsFor(properties.deadLetterTopic());
        if (info == null) {
            return List.of();
        }
        return info.stream()
                .map(partition -> new TopicPartition(partition.topic(), partition.partition()))
                .toList();
    }

    private DeadLetterRecordResponse toResponse(ConsumerRecord<String, String> record) {
        return new DeadLetterRecordResponse(
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                // Where it came from: the standard DLT headers, which are reliable.
                headerAsString(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                headerAsInt(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                headerAsLong(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                // Why it failed: the root cause written by the error handler, falling back
                // to the standard headers, which name Spring Kafka's wrapper instead.
                firstNonNull(headerAsString(record, KafkaConfig.FAILURE_TYPE_HEADER),
                        headerAsString(record, KafkaHeaders.DLT_EXCEPTION_FQCN)),
                firstNonNull(headerAsString(record, KafkaConfig.FAILURE_MESSAGE_HEADER),
                        headerAsString(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE)),
                reprocessAttemptsOf(record),
                Instant.ofEpochMilli(record.timestamp()));
    }

    static int reprocessAttemptsOf(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(OrderPublisher.REPROCESS_ATTEMPTS_HEADER);
        if (header == null || header.value() == null) {
            return 0;
        }
        try {
            return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static String firstNonNull(String preferred, String fallback) {
        return preferred != null ? preferred : fallback;
    }

    private static String headerAsString(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** The recoverer writes the numeric DLT headers as raw four- and eight-byte values. */
    private static Integer headerAsInt(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null || header.value().length < Integer.BYTES) {
            return null;
        }
        return java.nio.ByteBuffer.wrap(header.value()).getInt();
    }

    private static Long headerAsLong(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null || header.value().length < Long.BYTES) {
            return null;
        }
        return java.nio.ByteBuffer.wrap(header.value()).getLong();
    }
}
