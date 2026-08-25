-- Paging through an endpoint's history without `OFFSET`.
--
-- The listing sorts newest first and pages by carrying the last row's position
-- forward, which in SQL is the row comparison `(created_at, id) < ($1, $2)`. The
-- tiebreak on `id` is not decoration: `created_at` is not unique — a fan-out writes
-- every delivery for one event in the same transaction, so a busy endpoint can have
-- dozens sharing a timestamp — and paging on a non-unique key silently skips rows
-- and repeats others across page boundaries.
--
-- For that comparison to be an index seek rather than a filter, the index has to
-- carry `id` in the same order the query asks for it. The two-column index this
-- replaces stops one column short: it finds the timestamp and then scans every row
-- sharing it.
--
-- Dropped rather than kept alongside, because it is a strict prefix of the new one
-- and Postgres can answer anything it could answer. Two indexes on the same leading
-- columns cost writes on every insert to earn nothing.
CREATE INDEX deliveries_endpoint_created_id_idx
    ON deliveries (endpoint_id, created_at DESC, id DESC);

DROP INDEX deliveries_endpoint_created_idx;
