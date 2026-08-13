-- The attempt log becomes the delivery's full history rather than a rough tally.
--
-- Two things were missing for a delivery's history to be reconstructable from this
-- table alone: which process made the attempt, and what was decided afterwards.
-- Without the second, an attempt classified `retryable` that was actually the last
-- one is indistinguishable from one that was rescheduled.

ALTER TABLE delivery_attempts
    -- Which sender made this attempt. Nullable because rows written before this
    -- migration genuinely do not know, and inventing a value would be worse than
    -- admitting it.
    ADD COLUMN worker_id       text,
    -- When the retry was scheduled for, or NULL if this attempt was terminal. This
    -- is what makes the backoff auditable after the fact: the deliveries table only
    -- ever holds the *latest* schedule, so without this the earlier ones are lost.
    ADD COLUMN next_attempt_at timestamptz;

-- M1 wrote a single 'failed' class before the classifier existed. Those attempts
-- were all terminal, which is what 'permanent' means now.
UPDATE delivery_attempts SET outcome_class = 'permanent' WHERE outcome_class = 'failed';

-- Same reasoning as the status constraint on deliveries: a typo should be a failed
-- write, not a row that quietly breaks every dashboard that groups by this column.
--
-- 'deferred' is separate from 'retryable' on purpose. An endpoint answering 429 is
-- not failing — it is working correctly and asking us to slow down. Counting those
-- as errors makes a customer who is merely rate limited look like one who is broken,
-- which is the difference between an alert worth waking someone for and noise.
ALTER TABLE delivery_attempts
    ADD CONSTRAINT delivery_attempts_outcome_class_check
    CHECK (outcome_class IN ('success', 'deferred', 'retryable', 'permanent'));

-- The table is an audit ledger. Appending is the only legitimate operation: an
-- attempt that can be edited afterwards cannot be used as evidence of what happened,
-- and "we definitely sent it, look at the log" is most of what this table is for.
--
-- DELETE is deliberately still allowed, because the foreign key from deliveries
-- cascades and M8's retention will need to drop old rows in bulk.
CREATE OR REPLACE FUNCTION delivery_attempts_are_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION
        'delivery_attempts is append-only: attempt % of delivery % cannot be updated',
        OLD.attempt_no, OLD.delivery_id;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER delivery_attempts_no_update
    BEFORE UPDATE ON delivery_attempts
    FOR EACH ROW EXECUTE FUNCTION delivery_attempts_are_append_only();
