package com.gkcontas.orders.repository;

import com.gkcontas.orders.model.HandledDeadLetter;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HandledDeadLetterRepository extends JpaRepository<HandledDeadLetter, Long> {

    List<HandledDeadLetter> findAllByTopic(String topic);

    boolean existsByTopicAndPartitionIdAndRecordOffset(String topic, Integer partitionId, Long recordOffset);
}
