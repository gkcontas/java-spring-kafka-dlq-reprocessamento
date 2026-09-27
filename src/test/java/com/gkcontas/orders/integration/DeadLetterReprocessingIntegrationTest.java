package com.gkcontas.orders.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.gkcontas.orders.dto.DeadLetterRecordResponse;
import com.gkcontas.orders.dto.RecordCoordinates;
import com.gkcontas.orders.dto.ReprocessResultResponse;
import com.gkcontas.orders.exception.DeadLetterOperationException;
import com.gkcontas.orders.service.DemoFailureRules;
import java.util.List;
import org.junit.jupiter.api.Test;

class DeadLetterReprocessingIntegrationTest extends IntegrationTestBase {

    @Test
    void shouldProcessTheOrderWhenReplayAfterTheDependencyRecovers() {
        // Fails more times than the retry budget allows, so it lands in the dead letter
        // queue, and then succeeds when replayed. That is the shape of the outage this
        // endpoint exists for: a dependency down longer than a few hundred milliseconds of
        // backoff, fixed later by a human who then presses replay.
        String eventId = publish(DemoFailureRules.RECOVERING_ITEM, 5);
        DeadLetterRecordResponse deadLetter = awaitDeadLetter(eventId);

        ReprocessResultResponse result = deadLetterService.reprocess(
                List.of(new RecordCoordinates(deadLetter.partition(), deadLetter.offset())));

        assertThat(result.republished()).isEqualTo(1);
        assertThat(result.discarded()).isZero();
        awaitOrderPersisted(eventId);

        // The original dead letter is no longer outstanding: the log still holds it, but
        // the decision to reprocess was recorded, so the listing stops showing it.
        await().atMost(TIMEOUT).until(() -> findDeadLetter(eventId).isEmpty()
                || findDeadLetter(eventId).orElseThrow().offset() != deadLetter.offset());
    }

    @Test
    void shouldIncrementTheAttemptCounterWhenAReplayFailsAgain() {
        String eventId = publish(DemoFailureRules.ALWAYS_FAILING_ITEM, 1);
        DeadLetterRecordResponse first = awaitDeadLetter(eventId);
        assertThat(first.reprocessAttempts()).isZero();

        deadLetterService.reprocess(List.of(new RecordCoordinates(first.partition(), first.offset())));

        // A replay that fails produces a NEW dead letter, carrying the incremented counter
        // in a header. There is nowhere else to keep it that survives the round trip.
        await().atMost(TIMEOUT).until(() -> findDeadLetter(eventId)
                .map(record -> record.reprocessAttempts() == 1)
                .orElse(false));
    }

    @Test
    void shouldGiveUpOnceTheReprocessingCeilingIsReached() {
        String eventId = publish(DemoFailureRules.ALWAYS_FAILING_ITEM, 1);
        awaitDeadLetter(eventId);

        // The test profile sets the ceiling to 2. Without one, a record that replaying can
        // never fix circles between the topic and the queue for as long as anyone keeps
        // pressing the button, leaving a fresh dead letter each lap.
        for (int attempt = 0; attempt < 2; attempt++) {
            DeadLetterRecordResponse current = awaitDeadLetter(eventId);
            deadLetterService.reprocess(List.of(
                    new RecordCoordinates(current.partition(), current.offset())));
            int expected = attempt + 1;
            await().atMost(TIMEOUT).until(() -> findDeadLetter(eventId)
                    .map(record -> record.reprocessAttempts() == expected)
                    .orElse(false));
        }

        DeadLetterRecordResponse atCeiling = awaitDeadLetter(eventId);
        ReprocessResultResponse result = deadLetterService.reprocess(List.of(
                new RecordCoordinates(atCeiling.partition(), atCeiling.offset())));

        assertThat(result.republished()).isZero();
        assertThat(result.discarded()).isEqualTo(1);
        assertThat(result.notes()).anyMatch(note -> note.contains("ceiling"));
        assertThat(findDeadLetter(eventId)).isEmpty();
    }

    @Test
    void shouldListTheSameRecordsTwiceBecauseListingDoesNotConsume() {
        String eventId = publish(DemoFailureRules.ALWAYS_FAILING_ITEM, 1);
        awaitDeadLetter(eventId);

        List<DeadLetterRecordResponse> first = deadLetterService.list();
        List<DeadLetterRecordResponse> second = deadLetterService.list();

        // If the listing committed offsets — the default if you subscribe with the real
        // group id and let auto-commit run — the second call would come back empty and an
        // operator "just looking" would have marked the whole queue as consumed.
        assertThat(second).isNotEmpty();
        assertThat(second).extracting(DeadLetterRecordResponse::offset)
                .containsAll(first.stream().map(DeadLetterRecordResponse::offset).toList());
    }

    @Test
    void shouldHideADiscardedRecordFromTheListingWithoutRemovingItFromTheLog() {
        String eventId = publish(DemoFailureRules.ALWAYS_FAILING_ITEM, 1);
        DeadLetterRecordResponse deadLetter = awaitDeadLetter(eventId);

        deadLetterService.discard(deadLetter.partition(), deadLetter.offset(), "not worth replaying");

        assertThat(findDeadLetter(eventId)).isEmpty();
        // Kafka has no way to delete one record: the log is append-only and the only
        // removal mechanisms are retention and compaction, neither of which targets an
        // offset. "Discarded" means the decision was recorded, and a second attempt to
        // discard the same record now finds nothing outstanding.
        assertThatThrownBy(() -> deadLetterService.discard(
                deadLetter.partition(), deadLetter.offset(), "again"))
                .isInstanceOf(DeadLetterOperationException.class);
    }

    @Test
    void shouldReprocessNothingWhenTheSelectionMatchesNoOutstandingRecord() {
        ReprocessResultResponse result = deadLetterService.reprocess(
                List.of(new RecordCoordinates(0, Long.MAX_VALUE)));

        assertThat(result.republished()).isZero();
        assertThat(result.discarded()).isZero();
    }
}
