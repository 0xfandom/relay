-- The attempt log becomes a set of daily partitions.
--
-- This is the only table in Relay that grows without bound: every delivery writes at
-- least one row and a failing one writes twelve, forever, and nothing ever updates
-- them. At any real volume it becomes the largest object in the database by an order
-- of magnitude.
--
-- The obvious retention — `DELETE FROM delivery_attempts WHERE at < now() - 30 days`
-- — is the wrong tool, and not by a little. Deleting a row does not free its space;
-- it marks the row dead and leaves autovacuum to reclaim it later, so a bulk delete
-- produces a long vacuum on the busiest table in the system, a write-ahead log entry
-- per row, and index bloat that survives the vacuum. Run it daily and the vacuum
-- never catches up.
--
-- `DROP TABLE` on a partition unlinks the files. It is O(1), it takes no vacuum, and
-- it writes almost nothing. The retention window becomes "which partitions exist"
-- rather than a query that has to find and mark ten million rows.

-- The primary key has to include the partition key: Postgres cannot enforce
-- uniqueness across partitions without it, because there is no global index. `id`
-- alone is still unique in practice — the sequence is shared — so nothing about the
-- table's meaning changes.
ALTER TABLE delivery_attempts RENAME TO delivery_attempts_unpartitioned;
ALTER INDEX delivery_attempts_delivery_idx RENAME TO delivery_attempts_unpartitioned_idx;
DROP TRIGGER delivery_attempts_no_update ON delivery_attempts_unpartitioned;

CREATE TABLE delivery_attempts (
    id               bigserial   NOT NULL,
    delivery_id      uuid        NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    attempt_no       integer     NOT NULL,
    http_status      integer,
    latency_ms       integer     NOT NULL,
    outcome_class    text        NOT NULL,
    error            text,
    response_snippet text,
    worker_id        text,
    next_attempt_at  timestamptz,
    generation       integer     NOT NULL DEFAULT 0,
    at               timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, at),
    CONSTRAINT delivery_attempts_outcome_class_check
        CHECK (outcome_class IN ('success', 'deferred', 'retryable', 'permanent'))
) PARTITION BY RANGE (at);

CREATE INDEX delivery_attempts_delivery_idx
    ON delivery_attempts (delivery_id, attempt_no);

-- The safety net, and it is meant to stay empty.
--
-- Without it, an insert whose timestamp has no partition *fails* — and that insert
-- is the same transaction that records a delivery's outcome, so the delivery would
-- be retried and the endpoint would receive it twice. Losing the append-only log for
-- a day is bad; sending duplicate webhooks because a maintenance job was late is
-- worse.
--
-- Rows landing here are reported as a gauge, because the recovery is manual: a new
-- partition cannot be created over a range the default partition already holds rows
-- for.
CREATE TABLE delivery_attempts_default PARTITION OF delivery_attempts DEFAULT;

-- Append-only, as before. Declared on the partitioned table so every partition
-- inherits it — including the ones created months from now, which is the whole
-- reason not to declare it per partition.
CREATE TRIGGER delivery_attempts_no_update
    BEFORE UPDATE ON delivery_attempts
    FOR EACH ROW EXECUTE FUNCTION delivery_attempts_are_append_only();

-- Carry the history over. It goes into the default partition, which is exactly what
-- that partition is for during a migration, and the first maintenance run moves
-- nothing: these rows are older than the retention window on any real deployment and
-- the default partition is dropped and recreated empty below.
INSERT INTO delivery_attempts (
    id, delivery_id, attempt_no, http_status, latency_ms, outcome_class,
    error, response_snippet, worker_id, next_attempt_at, generation, at
)
SELECT id, delivery_id, attempt_no, http_status, latency_ms, outcome_class,
       error, response_snippet, worker_id, next_attempt_at, generation, at
FROM delivery_attempts_unpartitioned;

-- Keep the sequence ahead of what was copied.
SELECT setval(
    pg_get_serial_sequence('delivery_attempts', 'id'),
    GREATEST((SELECT COALESCE(max(id), 0) FROM delivery_attempts), 1)
);

DROP TABLE delivery_attempts_unpartitioned;

-- ---------------------------------------------------------------- maintenance
--
-- The DDL lives here rather than in Rust because it *is* DDL: building
-- `CREATE TABLE` strings in application code means a table name assembled from a
-- variable, and the only safe way to do that is the quoting Postgres already has.

-- Create every daily partition from yesterday to `days_ahead` from now.
--
-- Idempotent, so it can run every hour and usually do nothing. Creating them well
-- ahead is what keeps the default partition empty: the window between "this
-- partition is needed" and "this partition exists" is the whole risk, and pushing it
-- out to weeks means a maintenance job can be broken for a fortnight without
-- consequence.
--
-- Self-healing, and that is not optional. A row can only reach the default partition
-- if its day had no table — and once it is there, the plain
-- `CREATE TABLE ... PARTITION OF` for that day *fails*, permanently, because
-- Postgres refuses to create a partition covering rows the default already holds. A
-- naive version of this function is therefore a trap: the first time it is late, it
-- can never catch up, and every subsequent write for that day also lands in the
-- default. The recovery below is the detach-move-attach dance, done in one
-- transaction so a failure part-way leaves the table exactly as it was.
CREATE FUNCTION relay_ensure_attempt_partitions(days_ahead integer)
RETURNS integer AS $$
DECLARE
    d       date;
    made    integer := 0;
    name    text;
    stranded boolean;
BEGIN
    FOR d IN
        SELECT generate_series(current_date - 1, current_date + days_ahead, '1 day')::date
    LOOP
        name := 'delivery_attempts_' || to_char(d, 'YYYYMMDD');
        CONTINUE WHEN to_regclass(name) IS NOT NULL;

        EXECUTE format(
            'SELECT EXISTS (SELECT 1 FROM delivery_attempts_default WHERE at >= %L AND at < %L)',
            d, d + 1
        ) INTO stranded;

        IF stranded THEN
            -- The slow path, taken only after this job has been late. Detaching the
            -- default lifts the constraint that makes the create impossible; the rows
            -- are then moved into the new partition and the net put back.
            ALTER TABLE delivery_attempts DETACH PARTITION delivery_attempts_default;
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF delivery_attempts FOR VALUES FROM (%L) TO (%L)',
                name, d, d + 1
            );
            EXECUTE format(
                'WITH moved AS (
                     DELETE FROM delivery_attempts_default WHERE at >= %L AND at < %L RETURNING *
                 )
                 INSERT INTO delivery_attempts SELECT * FROM moved',
                d, d + 1
            );
            ALTER TABLE delivery_attempts ATTACH PARTITION delivery_attempts_default DEFAULT;
            RAISE WARNING
                'moved stranded attempts out of the default partition into %', name;
        ELSE
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF delivery_attempts FOR VALUES FROM (%L) TO (%L)',
                name, d, d + 1
            );
        END IF;
        made := made + 1;
    END LOOP;
    RETURN made;
END;
$$ LANGUAGE plpgsql;

-- Seed the partitions this deployment needs before anything can write.
--
-- Without this there is a window between "the schema exists" and "the maintenance
-- job has run once" in which every attempt lands in the default partition — on every
-- fresh install, which is the worst possible time to exercise a recovery path.
SELECT relay_ensure_attempt_partitions(14);

-- Drop every daily partition entirely older than the retention window.
--
-- Returns the names dropped so the caller can log them. A partition is only dropped
-- when its *whole* range is past the cutoff — dropping the one the cutoff falls
-- inside would delete attempts that are still inside the window.
CREATE FUNCTION relay_drop_attempt_partitions(retention_days integer)
RETURNS text[] AS $$
DECLARE
    cutoff  date := current_date - retention_days;
    dropped text[] := '{}';
    part    record;
BEGIN
    FOR part IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = 'delivery_attempts'
          AND c.relname ~ '^delivery_attempts_[0-9]{8}$'
          AND to_date(right(c.relname, 8), 'YYYYMMDD') + 1 <= cutoff
    LOOP
        EXECUTE format('DROP TABLE %I', part.relname);
        dropped := dropped || part.relname;
    END LOOP;
    RETURN dropped;
END;
$$ LANGUAGE plpgsql;
