-- The mark that stops the outbox publisher announcing the same row forever.
--
-- A column rather than a new `status` value, and that is the important choice. A
-- `queued` status would have to be understood by the claim query, the queue gauges,
-- the retention sweep and every dashboard that groups by status — and polling mode,
-- which is the default and does not have an outbox at all, would inherit all of it.
-- A nullable timestamp is invisible to everything that does not ask for it.
ALTER TABLE deliveries ADD COLUMN queued_at timestamptz;

-- What the publisher selects: due, pending, and not yet announced.
--
-- Deliberately narrower than the claim's index and not a substitute for it. Postgres
-- can only use a partial index when the query's predicate implies the index's, and
-- the claim never mentions `queued_at` — so this cannot become a second candidate
-- for the hottest query in the system. (That has happened here before: a broad
-- partial index that also satisfied the claim made the planner start choosing it.)
CREATE INDEX deliveries_unqueued_idx
    ON deliveries (next_attempt_at)
    WHERE status = 'pending' AND queued_at IS NULL;

-- Clearing the mark is an invariant, so it lives here rather than in five queries.
--
-- Any row returning to `pending` needs announcing again: a retry after a failure, a
-- deferral, a lease reaped from a dead worker, a shutdown release, a replay from the
-- dead letter queue. That is five code paths today and the set grows. Every one of
-- them that forgot would leave a delivery marked as announced with no message in the
-- broker — pending forever, invisible, and needing the reconciliation sweep to
-- notice. Enforced once, here, it cannot be forgotten by code written later.
CREATE OR REPLACE FUNCTION relay_clear_queued_at() RETURNS trigger AS $$
BEGIN
    NEW.queued_at := NULL;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deliveries_clear_queued_at
    BEFORE UPDATE ON deliveries
    FOR EACH ROW
    -- Only on the transition *into* pending. Without this the publisher's own
    -- `UPDATE ... SET queued_at = now()` would fire the trigger and immediately
    -- undo itself.
    WHEN (NEW.status = 'pending' AND OLD.status IS DISTINCT FROM 'pending')
    EXECUTE FUNCTION relay_clear_queued_at();
