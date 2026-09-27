CREATE TABLE orders (
    id              BIGSERIAL      PRIMARY KEY,
    -- The deduplication key, and the reason it is UNIQUE rather than merely indexed.
    -- Kafka delivers at-least-once, and the reprocessing endpoint deliberately replays
    -- records, so the same event WILL arrive twice. A check-then-insert in application
    -- code loses that race; the constraint does not.
    event_id        VARCHAR(64)    NOT NULL UNIQUE,
    customer_email  VARCHAR(200)   NOT NULL,
    item            VARCHAR(120)   NOT NULL,
    quantity        INTEGER        NOT NULL,
    unit_price      NUMERIC(12, 2) NOT NULL,
    total_amount    NUMERIC(12, 2) NOT NULL,
    processed_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- Kafka cannot delete a single record: a topic is an append-only log, and the only
-- removal mechanisms are retention and compaction, neither of which targets one offset.
-- So "handling" a dead letter means recording the decision here and having the listing
-- skip it from then on. Anything else would be pretending the record went away.
CREATE TABLE handled_dead_letters (
    id            BIGSERIAL    PRIMARY KEY,
    topic         VARCHAR(200) NOT NULL,
    partition_id  INTEGER      NOT NULL,
    record_offset BIGINT       NOT NULL,
    disposition   VARCHAR(20)  NOT NULL CHECK (disposition IN ('REPROCESSED', 'DISCARDED')),
    reason        VARCHAR(500),
    handled_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_handled_dead_letter UNIQUE (topic, partition_id, record_offset)
);
