-- Recognise a producer's retry as the same request.
--
-- A producer that POSTs an event and never sees the reply cannot tell whether we
-- received it. Not retrying loses the webhook; retrying sends it twice. The way out
-- is for the producer to name its request, so a retry arrives carrying the same
-- name and can be answered with the original result instead of creating a second
-- event.

CREATE TABLE idempotency_keys (
    -- The caller's own name for the request, and the primary key: the uniqueness
    -- *is* the mechanism. Two concurrent inserts of one key cannot both succeed, so
    -- Postgres decides the winner rather than application code trying to.
    --
    -- Global today because Relay has one tenant. When tenancy lands this becomes
    -- PRIMARY KEY (tenant_id, key) — a shared key space across customers would let
    -- one customer's choice of key silently swallow another's event.
    key            text        PRIMARY KEY,

    -- What the first request produced. Cascades because a key that outlives its
    -- event can only answer with a reference to something that is gone.
    event_id       uuid        NOT NULL REFERENCES events(id) ON DELETE CASCADE,

    -- SHA-256 of the event type and body the key was first used for.
    --
    -- Guards against the caller reusing one key for two different requests. Without
    -- it the second request is answered with the first one's result and silently
    -- dropped — a lost event that looks to the caller like a success. With it, the
    -- caller gets an error naming their own bug.
    request_digest bytea       NOT NULL,

    -- The exact bytes of the 202 body returned the first time.
    --
    -- bytea and stored verbatim, for the same reason events.raw_payload is: a
    -- duplicate must get a byte-identical answer, and reconstructing the JSON would
    -- reserialise it with no guarantee of the same key order or the same delivery
    -- id ordering.
    response       bytea       NOT NULL,

    created_at     timestamptz NOT NULL DEFAULT now()
);

-- Keys are kept for a window, not forever, so the pruner needs to find the oldest
-- ones without scanning the table. The window is the trade this design makes: a
-- duplicate arriving after it expires creates a second event.
CREATE INDEX idempotency_keys_created_idx ON idempotency_keys (created_at);
