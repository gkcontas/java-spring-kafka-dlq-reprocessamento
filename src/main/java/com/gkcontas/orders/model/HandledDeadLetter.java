package com.gkcontas.orders.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A decision taken about one dead letter, identified by its coordinates in the log.
 *
 * <p>This table exists because Kafka has no way to remove a single record. The log keeps
 * the dead letter forever (until retention); what changes is whether this application
 * still considers it outstanding.
 */
@Entity
@Table(name = "handled_dead_letters")
@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class HandledDeadLetter {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 200)
    private String topic;

    @Column(name = "partition_id", nullable = false)
    private Integer partitionId;

    @Column(name = "record_offset", nullable = false)
    private Long recordOffset;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private Disposition disposition;

    @Column(length = 500)
    private String reason;

    @Column(name = "handled_at", nullable = false)
    private Instant handledAt;

    public HandledDeadLetter(String topic, int partitionId, long recordOffset,
                             Disposition disposition, String reason) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.recordOffset = recordOffset;
        this.disposition = disposition;
        this.reason = reason;
        this.handledAt = Instant.now();
    }
}
