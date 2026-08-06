-- Relay's initial schema.
--
-- The load-bearing decision here is that an EVENT is not a DELIVERY. One event
-- fanning out to three endpoints produces three delivery rows, each with its own
-- status, attempt counter and next-attempt time, because each has an independent
-- fate: one succeeds immediately, one needs six retries, one is permanently dead.

-- Who we send to.
CREATE TABLE endpoints (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    url          text        NOT NULL,
    secret       text        NOT NULL,
    event_types  text[]      NOT NULL DEFAULT '{}',   -- empty = subscribe to everything
    enabled      boolean     NOT NULL DEFAULT true,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- What happened. Recorded once, regardless of how many endpoints want it.
CREATE TABLE events (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type   text        NOT NULL,
    -- bytea, deliberately NOT jsonb: the signature covers the exact bytes that
    -- arrived. jsonb normalises and can reorder keys, which would produce a
    -- different byte sequence and invalidate every signature.
    raw_payload  bytea       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- The unit of work: one row per (event x endpoint).
CREATE TABLE deliveries (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        uuid        NOT NULL REFERENCES events(id)    ON DELETE CASCADE,
    endpoint_id     uuid        NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    status          text        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending','inflight','succeeded','dead')),
    attempt         integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    -- Lease columns. Unused until M2, but the claim query and the reaper are the
    -- reason the table exists in this shape, so they are here from the start.
    locked_at       timestamptz,
    locked_by       text,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- The working set is tiny compared to the table: after a month there may be ten
-- million succeeded rows and a few hundred pending ones, and the dispatcher only
-- ever asks "what is pending and due?". A partial index covers only those rows,
-- so it stays small enough to live in memory permanently.
CREATE INDEX deliveries_pending_due_idx
    ON deliveries (next_attempt_at)
    WHERE status = 'pending';

-- Useful for the delivery-history API in M7, and for debugging now.
CREATE INDEX deliveries_endpoint_created_idx
    ON deliveries (endpoint_id, created_at DESC);

-- The receipt book. Append-only: never updated, never individually deleted.
-- It is simultaneously the audit log, the retry history and the latency dataset.
CREATE TABLE delivery_attempts (
    id               bigserial   PRIMARY KEY,
    delivery_id      uuid        NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    attempt_no       integer     NOT NULL,
    http_status      integer,
    latency_ms       integer     NOT NULL,
    outcome_class    text        NOT NULL,
    error            text,
    -- Customer error pages can be enormous; the sender truncates before storing.
    response_snippet text,
    at               timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX delivery_attempts_delivery_idx
    ON delivery_attempts (delivery_id, attempt_no);
