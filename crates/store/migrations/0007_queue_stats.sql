-- Make "how is the queue doing" answerable without reading the whole table.
--
-- The metrics endpoint asks for the open set — pending, inflight and dead — every
-- time Prometheus scrapes, which is every fifteen seconds forever. Succeeded rows
-- are the overwhelming majority and are never part of the answer, so a plain
-- `GROUP BY status` would scan millions of rows to count a few hundred.
--
-- Pending and dead already have partial indexes, added for the claim and the dead
-- letter listing. Inflight is the one status with none, because until now nothing
-- ever asked for it as a set: the claim writes it and the reaper finds rows by
-- lease age. This closes that gap.
--
-- Deliberately narrow, and deliberately *not* a general index over everything that
-- is not succeeded. A broader index would also satisfy the claim's predicate, and
-- the planner would then have two candidates for the hottest query in the system —
-- one purpose-built and one merely adequate. Postgres picks by estimated cost, so a
-- statistics wobble is enough to switch it to the wrong one, and a scan that grows
-- with the whole open set is exactly what `deliveries_pending_due_idx` exists to
-- prevent. Splitting the predicate by status keeps every index serving one question.
CREATE INDEX deliveries_inflight_idx
    ON deliveries (locked_at)
    WHERE status = 'inflight';
