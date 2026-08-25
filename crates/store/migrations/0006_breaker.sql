-- Circuit breaker state, per endpoint.
--
-- On the endpoint row rather than in process memory, and that is the whole point of
-- this migration. In-process state looks correct with one worker and silently fails
-- with several: each sees a fraction of the failures, none of them reaches the
-- threshold, and the breaker never trips while every worker independently concludes
-- the endpoint is merely unlucky. Two dispatcher replicas make it worse again.
--
-- Postgres is already the thing every worker agrees on, so it is where the agreement
-- goes.

ALTER TABLE endpoints
    ADD COLUMN breaker_state text NOT NULL DEFAULT 'closed'
        CHECK (breaker_state IN ('closed', 'open', 'half_open')),

    -- Consecutive failures, reset by any answer from the endpoint. Consecutive
    -- rather than cumulative: an endpoint that fails one request in five is
    -- unhealthy in a way retries already handle, and a cumulative count would
    -- eventually trip on every endpoint that has ever failed.
    ADD COLUMN consecutive_failures integer NOT NULL DEFAULT 0
        CHECK (consecutive_failures >= 0),

    -- How many times this breaker has opened without a successful probe in between.
    -- Drives how long the next cooldown is, and is cleared by a probe that works.
    ADD COLUMN breaker_trips integer NOT NULL DEFAULT 0
        CHECK (breaker_trips >= 0),

    -- When a probe may next be issued. NULL unless the breaker is open.
    --
    -- A timestamp rather than a duration because the deciding is done in SQL: the
    -- probe is claimed by a conditional UPDATE that compares this to `now()`, so the
    -- database picks the single winner instead of application code racing to.
    ADD COLUMN breaker_probe_at timestamptz,

    -- When the breaker last opened. Not used for any decision — it is here so that
    -- "how long has this endpoint been cut off" is answerable without reconstructing
    -- it from the attempt log.
    ADD COLUMN breaker_opened_at timestamptz;

-- A breaker that is not closed must have a probe time, and one that is closed must
-- not. An open breaker with no probe time would never be probed again, and the
-- endpoint would be cut off permanently by a bug that looks like a NULL.
--
-- Half-open carries one too: it is the deadline by which the in-flight probe must
-- have reported, after which another may be issued. Without it a probe against an
-- endpoint that accepts connections and never answers would leave the breaker
-- half-open forever, which is the same permanent cut-off by a different route.
ALTER TABLE endpoints
    ADD CONSTRAINT endpoints_live_breaker_has_probe_time
    CHECK ((breaker_state = 'closed') = (breaker_probe_at IS NULL));
