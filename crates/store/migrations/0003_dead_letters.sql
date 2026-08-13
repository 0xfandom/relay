-- Give up with a reason, and be able to change your mind.
--
-- A dead letter queue that cannot be drained is only a log file. The point of
-- parking a delivery rather than dropping it is that the underlying problem usually
-- gets fixed — the endpoint comes back, the URL is corrected — and the deliveries
-- that failed in the meantime are still owed.

ALTER TABLE deliveries
    -- Why we stopped. 'permanent_failure' means the first attempt already showed it
    -- would never work; 'attempts_exhausted' means it might have, and we ran out of
    -- tries. Operationally those need completely different responses, and without
    -- this column they are indistinguishable.
    ADD COLUMN dead_reason text,
    -- How many times this delivery has been replayed.
    --
    -- Replay resets the attempt counter, which would otherwise make the attempt log
    -- ambiguous: a second attempt 0 for the same delivery, with no way to tell which
    -- run it belonged to. Stamping the generation on every attempt keeps
    -- (generation, attempt_no) unique and the history readable.
    ADD COLUMN generation integer NOT NULL DEFAULT 0;

ALTER TABLE delivery_attempts
    ADD COLUMN generation integer NOT NULL DEFAULT 0;

-- Rows that died before this column existed did so on a permanent failure: there
-- were no retries then, so nothing could exhaust them.
UPDATE deliveries SET dead_reason = 'permanent_failure' WHERE status = 'dead';

-- A biconditional rather than two separate rules. A dead delivery with no reason
-- cannot be triaged, and a live delivery carrying one is a replay that forgot to
-- clear it — which would then show up in the dead letter listing while quietly
-- being delivered.
ALTER TABLE deliveries
    ADD CONSTRAINT deliveries_dead_reason_check
    CHECK ((status = 'dead') = (dead_reason IS NOT NULL)),
    ADD CONSTRAINT deliveries_dead_reason_values_check
    CHECK (dead_reason IS NULL OR dead_reason IN ('permanent_failure', 'attempts_exhausted'));

-- The dead set is small next to the whole table and is the only thing this listing
-- ever reads, so it gets its own partial index for the same reason the pending set
-- does.
CREATE INDEX deliveries_dead_idx
    ON deliveries (created_at DESC)
    WHERE status = 'dead';
