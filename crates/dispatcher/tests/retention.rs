//! Keeping the attempt log from growing forever.
//!
//! The attempt log is the only table in Relay that grows with *traffic* rather than
//! with customers: every delivery writes a row and a failing one writes twelve. At
//! any real volume it becomes the largest object in the database by an order of
//! magnitude.
//!
//! The obvious retention — one big `DELETE` — is the wrong tool. Deleting a row does
//! not free its space; it marks the row dead and leaves autovacuum to reclaim it, so
//! a bulk delete produces a long vacuum on the busiest table in the system, a
//! write-ahead log record per row, and index bloat that outlives the vacuum. Run it
//! daily and the vacuum never catches up.
//!
//! Dropping a partition unlinks files. These tests are about proving that is what
//! actually happens, not that "old rows go away" — because a `DELETE` would pass
//! that weaker test while causing the problem.
//!
//! Requires Postgres: `docker compose up -d`.

use std::time::Duration;

use relay_dispatcher::{Pruner, PrunerConfig};
use relay_store::{AttemptResult, DeadReason, DeliveryStatus, Store};
use sqlx::{PgPool, Row};
use uuid::Uuid;

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

fn config(store_days: u64) -> PrunerConfig {
    PrunerConfig {
        attempts: DAY * store_days as u32,
        succeeded: DAY * store_days as u32,
        dead: DAY * 90,
        batch: 100,
        partition_days_ahead: 3,
        interval: Duration::from_secs(3600),
        ..PrunerConfig::default()
    }
}

/// Queue one delivery and return its id.
async fn queue(store: &Store) -> Uuid {
    let event_type = format!("ret.{}", Uuid::new_v4());
    store
        .create_endpoint(
            "https://example.com/hook",
            "whsec_retention_test",
            std::slice::from_ref(&event_type),
        )
        .await
        .expect("endpoint");
    store
        .insert_event_and_fan_out(&event_type, br#"{"n":1}"#)
        .await
        .expect("insert")
        .delivery_ids[0]
}

/// Write one attempt row, stamped `days_ago`.
///
/// Stamped after the fact rather than by moving a clock: the column defaults to
/// `now()`, and the property being tested is which partition a timestamp lands in.
async fn attempt_at(store: &Store, delivery: Uuid, attempt_no: i32, days_ago: i64) {
    store
        .finish_attempt(
            delivery,
            attempt_no,
            AttemptResult::Retry {
                delay: Duration::from_secs(3600),
            },
            Some(503),
            10,
            "retryable",
            Some("HTTP 503"),
            None,
            "worker-a",
        )
        .await
        .expect("attempt");
    // The trigger forbids UPDATE, so the row is rewritten into the right partition
    // instead: delete and re-insert with the timestamp under test.
    sqlx::query(
        "WITH moved AS (
             DELETE FROM delivery_attempts
             WHERE delivery_id = $1 AND attempt_no = $2
             RETURNING delivery_id, attempt_no, http_status, latency_ms, outcome_class,
                       error, response_snippet, worker_id, next_attempt_at, generation
         )
         INSERT INTO delivery_attempts (
             delivery_id, attempt_no, http_status, latency_ms, outcome_class,
             error, response_snippet, worker_id, next_attempt_at, generation, at
         )
         SELECT delivery_id, attempt_no, http_status, latency_ms, outcome_class,
                error, response_snippet, worker_id, next_attempt_at, generation,
                now() - make_interval(days => $3)
         FROM moved",
    )
    .bind(delivery)
    .bind(attempt_no)
    .bind(days_ago as i32)
    .execute(store.pool())
    .await
    .expect("restamp");
}

async fn partition_names(store: &Store) -> Vec<String> {
    sqlx::query(
        "SELECT c.relname::text AS name
         FROM pg_class c
         JOIN pg_inherits i ON i.inhrelid = c.oid
         JOIN pg_class p ON p.oid = i.inhparent
         WHERE p.relname = 'delivery_attempts'
         ORDER BY 1",
    )
    .fetch_all(store.pool())
    .await
    .expect("partitions")
    .into_iter()
    .map(|r| r.get::<String, _>("name"))
    .collect()
}

async fn attempt_count(store: &Store) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM delivery_attempts")
        .fetch_one(store.pool())
        .await
        .expect("count")
}

// ---------------------------------------------------------------- partitioning

#[sqlx::test(migrations = "../store/migrations")]
async fn the_attempt_log_is_partitioned_by_day(pool: PgPool) {
    let store = Store::from_pool(pool);

    let kind: String = sqlx::query_scalar(
        "SELECT c.relkind::text FROM pg_class c WHERE c.relname = 'delivery_attempts'",
    )
    .fetch_one(store.pool())
    .await
    .expect("relkind");
    // `p`, not `r`. If this ever reads `r` somebody has recreated the table as a
    // plain one and every retention guarantee below is silently gone.
    assert_eq!(kind, "p", "delivery_attempts must be a partitioned table");

    let strategy: String = sqlx::query_scalar(
        "SELECT pg_get_partkeydef(c.oid) FROM pg_class c WHERE c.relname = 'delivery_attempts'",
    )
    .fetch_one(store.pool())
    .await
    .expect("partition key");
    assert_eq!(strategy, "RANGE (at)");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn partitions_exist_before_anything_can_write(pool: PgPool) {
    let store = Store::from_pool(pool);

    // Seeded by the migration, not by the first maintenance run. Otherwise every
    // fresh install has a window between "the schema exists" and "the job has run
    // once" in which every attempt lands in the default partition — which is the
    // worst possible time to be exercising a recovery path.
    assert!(
        store.attempt_partitions().await.expect("count") >= 15,
        "the migration should seed the partitions this deployment needs"
    );

    // And the sweep is idempotent: it runs hourly and must do nothing almost every
    // time.
    let pruner = Pruner::new(store.clone(), config(30));
    assert_eq!(
        pruner.prune_once().await.expect("sweep").partitions_created,
        0
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn asking_for_a_longer_margin_creates_the_difference(pool: PgPool) {
    let store = Store::from_pool(pool);
    let before = store.attempt_partitions().await.expect("count");

    let mut cfg = config(30);
    cfg.partition_days_ahead = 20;
    let swept = Pruner::new(store.clone(), cfg)
        .prune_once()
        .await
        .expect("sweep");

    // The margin is the whole point: a row can only reach the default partition if
    // its day has no table, so creating days ahead means this job can be broken for
    // that long without consequence.
    assert_eq!(swept.partitions_created, 6);
    assert_eq!(store.attempt_partitions().await.expect("count"), before + 6);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_row_stranded_in_the_default_partition_is_rescued(pool: PgPool) {
    let store = Store::from_pool(pool);
    let id = queue(&store).await;

    // A write for a day beyond the seeded margin, which is what happens when this
    // job has been broken for longer than the margin. It lands in the default
    // partition, and from that moment the plain `CREATE TABLE ... PARTITION OF` for
    // that day fails *permanently* — Postgres refuses to create a partition covering
    // rows the default already holds. A naive maintainer can never catch up.
    attempt_at(&store, id, 0, -30).await;
    assert_eq!(store.attempts_in_default_partition().await.unwrap(), 1);

    let mut cfg = config(30);
    cfg.partition_days_ahead = 40;
    let swept = Pruner::new(store.clone(), cfg)
        .prune_once()
        .await
        .expect("the sweep must recover rather than fail");

    assert!(swept.partitions_created > 0);
    // Rescued into its own day's partition, not left behind and not lost.
    assert_eq!(
        store.attempts_in_default_partition().await.unwrap(),
        0,
        "the stranded row should have been moved"
    );
    assert_eq!(attempt_count(&store).await, 1, "and not deleted on the way");
    let landed_in: String =
        sqlx::query_scalar("SELECT tableoid::regclass::text FROM delivery_attempts")
            .fetch_one(store.pool())
            .await
            .expect("partition");
    assert_ne!(landed_in, "delivery_attempts_default");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_write_lands_in_its_own_days_partition(pool: PgPool) {
    let store = Store::from_pool(pool);
    Pruner::new(store.clone(), config(30))
        .prune_once()
        .await
        .expect("sweep");
    let id = queue(&store).await;
    attempt_at(&store, id, 0, 0).await;

    // Asked of the row rather than by building a table name into a query: `tableoid`
    // is the partition the row actually landed in, which is the thing under test.
    let landed_in: String =
        sqlx::query_scalar("SELECT tableoid::regclass::text FROM delivery_attempts")
            .fetch_one(store.pool())
            .await
            .expect("the partition the row landed in");
    let expected: String =
        sqlx::query_scalar("SELECT 'delivery_attempts_' || to_char(current_date, 'YYYYMMDD')")
            .fetch_one(store.pool())
            .await
            .expect("today's partition name");
    assert_eq!(landed_in, expected);

    // The safety net stayed empty, which is the only acceptable state for it.
    assert_eq!(store.attempts_in_default_partition().await.unwrap(), 0);
}

// ------------------------------------------------------------------- retention

#[sqlx::test(migrations = "../store/migrations")]
async fn a_fresh_database_loses_nothing(pool: PgPool) {
    let store = Store::from_pool(pool);
    let id = queue(&store).await;
    attempt_at(&store, id, 0, 0).await;
    assert_eq!(attempt_count(&store).await, 1);

    let swept = Pruner::new(store.clone(), config(10))
        .prune_once()
        .await
        .expect("sweep");

    // A retention job that removes something on a fresh database is worse than one
    // that removes nothing: it means the window is being read wrong, and the same
    // bug on a real deployment eats a month of history.
    assert!(swept.partitions_dropped.is_empty());
    assert_eq!(swept.succeeded_deleted, 0);
    assert_eq!(swept.dead_deleted, 0);
    assert_eq!(attempt_count(&store).await, 1);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_partition_whose_whole_range_is_expired_is_dropped(pool: PgPool) {
    let store = Store::from_pool(pool);
    let id = queue(&store).await;

    // Real dated partitions covering 40 days back to today.
    sqlx::query(
        "DO $$
         DECLARE d date;
         BEGIN
           FOR d IN SELECT generate_series(current_date - 40, current_date, '1 day')::date LOOP
             EXECUTE format(
               'CREATE TABLE IF NOT EXISTS delivery_attempts_%s PARTITION OF delivery_attempts
                FOR VALUES FROM (%L) TO (%L)',
               to_char(d, 'YYYYMMDD'), d, d + 1);
           END LOOP;
         END $$",
    )
    .execute(store.pool())
    .await
    .expect("backfill partitions");

    attempt_at(&store, id, 0, 35).await; // outside a 10-day window
    attempt_at(&store, id, 1, 2).await; // inside it
    assert_eq!(attempt_count(&store).await, 2);

    let before = partition_names(&store).await.len();
    let swept = Pruner::new(store.clone(), config(10))
        .prune_once()
        .await
        .expect("sweep");

    // The claim that matters: rows left by *dropping tables*, not by deleting rows.
    assert!(
        !swept.partitions_dropped.is_empty(),
        "expired attempts must be removed by dropping partitions"
    );
    assert!(partition_names(&store).await.len() < before);
    // The old row is gone and the recent one is untouched.
    assert_eq!(attempt_count(&store).await, 1);
    let remaining: i32 = sqlx::query_scalar("SELECT attempt_no FROM delivery_attempts")
        .fetch_one(store.pool())
        .await
        .expect("the surviving attempt");
    assert_eq!(remaining, 1, "the attempt inside the window was dropped");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_partition_the_cutoff_falls_inside_is_kept(pool: PgPool) {
    let store = Store::from_pool(pool);
    sqlx::query(
        "DO $$
         DECLARE d date;
         BEGIN
           FOR d IN SELECT generate_series(current_date - 20, current_date, '1 day')::date LOOP
             EXECUTE format(
               'CREATE TABLE IF NOT EXISTS delivery_attempts_%s PARTITION OF delivery_attempts
                FOR VALUES FROM (%L) TO (%L)',
               to_char(d, 'YYYYMMDD'), d, d + 1);
           END LOOP;
         END $$",
    )
    .execute(store.pool())
    .await
    .expect("backfill");

    let id = queue(&store).await;
    // Exactly on the boundary of a 10-day window. Dropping the partition it lives in
    // would delete attempts that are still inside the window, which is data loss
    // dressed up as retention.
    attempt_at(&store, id, 0, 10).await;

    Pruner::new(store.clone(), config(10))
        .prune_once()
        .await
        .expect("sweep");
    assert_eq!(
        attempt_count(&store).await,
        1,
        "an attempt on the boundary must survive"
    );
}

// -------------------------------------------------------------- per-table windows

#[sqlx::test(migrations = "../store/migrations")]
async fn deliveries_and_dead_letters_are_kept_on_different_schedules(pool: PgPool) {
    let store = Store::from_pool(pool);
    let succeeded = queue(&store).await;
    let dead = queue(&store).await;
    let pending = queue(&store).await;

    store
        .finish_attempt(
            succeeded,
            0,
            AttemptResult::Succeeded,
            Some(200),
            5,
            "success",
            None,
            None,
            "w",
        )
        .await
        .expect("succeed");
    store
        .finish_attempt(
            dead,
            0,
            AttemptResult::Dead {
                reason: DeadReason::PermanentFailure,
            },
            Some(404),
            5,
            "permanent",
            Some("HTTP 404"),
            None,
            "w",
        )
        .await
        .expect("die");

    // All three well past the 30-day succeeded window, and inside the 90-day dead one.
    sqlx::query("UPDATE deliveries SET created_at = now() - interval '45 days'")
        .execute(store.pool())
        .await
        .expect("backdate");

    let swept = Pruner::new(store.clone(), config(30))
        .prune_once()
        .await
        .expect("sweep");

    assert_eq!(swept.succeeded_deleted, 1);
    // A dead letter is a webhook somebody is still owed, and the whole point of the
    // queue is that it can be replayed once the underlying problem is fixed. It
    // outlives the successes by design.
    assert_eq!(swept.dead_deleted, 0);

    let left: Vec<String> = sqlx::query_scalar("SELECT status FROM deliveries ORDER BY status")
        .fetch_all(store.pool())
        .await
        .expect("statuses");
    // And a pending delivery is never deleted however old it is: it is still owed.
    assert_eq!(left, vec!["dead".to_string(), "pending".to_string()]);
    let _ = pending;
}

#[sqlx::test(migrations = "../store/migrations")]
async fn deletion_is_batched_rather_than_one_enormous_statement(pool: PgPool) {
    let store = Store::from_pool(pool);
    let event_type = format!("ret.{}", Uuid::new_v4());
    store
        .create_endpoint(
            "https://example.com/hook",
            "whsec_retention_test",
            std::slice::from_ref(&event_type),
        )
        .await
        .expect("endpoint");
    for _ in 0..25 {
        let id = store
            .insert_event_and_fan_out(&event_type, br#"{"n":1}"#)
            .await
            .expect("insert")
            .delivery_ids[0];
        store
            .finish_attempt(
                id,
                0,
                AttemptResult::Succeeded,
                Some(200),
                5,
                "success",
                None,
                None,
                "w",
            )
            .await
            .expect("succeed");
    }
    sqlx::query("UPDATE deliveries SET created_at = now() - interval '45 days'")
        .execute(store.pool())
        .await
        .expect("backdate");

    // One batch at a time. A single statement covering a month of history holds a
    // transaction and a pile of row locks on the table the claim query reads, and a
    // delivery waiting behind a retention sweep is a webhook arriving late for a
    // reason no customer could ever be told.
    assert_eq!(
        store
            .delete_deliveries(DeliveryStatus::Succeeded, DAY * 30, 10)
            .await
            .expect("delete"),
        10
    );

    // And the sweep loops until the backlog is gone rather than leaving it for the
    // next hour.
    let mut cfg = config(30);
    cfg.batch = 10;
    let swept = Pruner::new(store.clone(), cfg)
        .prune_once()
        .await
        .expect("sweep");
    assert_eq!(swept.succeeded_deleted, 15);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM deliveries")
            .fetch_one(store.pool())
            .await
            .expect("count"),
        0
    );
}

// ------------------------------------------------------------------- the gauge

#[sqlx::test(migrations = "../store/migrations")]
async fn every_table_reports_its_size(pool: PgPool) {
    let store = Store::from_pool(pool);
    let sizes = store.table_sizes().await.expect("sizes");

    for expected in [
        "deliveries",
        "events",
        "endpoints",
        "delivery_attempts",
        "idempotency_keys",
    ] {
        let found = sizes.iter().find(|t| t.table_name == expected);
        let found = found.unwrap_or_else(|| panic!("{expected} is not reported"));
        // Per table, not a total: a total cannot say which one stopped being pruned,
        // and the answer is almost always the attempt log.
        assert!(found.bytes >= 0);
    }
}
