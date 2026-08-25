//! Tests for the concurrent claim.
//!
//! The property that matters is that two workers running `claim_batch` at the same
//! instant never receive the same delivery. Everything downstream assumes
//! exclusivity, so if the claim is wrong every endpoint gets duplicates and no
//! later test will tell you why.
//!
//! Each test runs against its own database, created and dropped by `#[sqlx::test]`.
//! Sharing one database made these tests claim each other's rows and fail for
//! reasons unrelated to the code — the isolation is not a nicety here, it is what
//! makes the assertions mean anything.
//!
//! Requires Postgres: `docker compose up -d`.

use relay_store::Store;
use sqlx::PgPool;
use uuid::Uuid;

/// Create `n` pending deliveries against one endpoint.
async fn seed(store: &Store, n: usize) -> Vec<Uuid> {
    store
        .create_endpoint("http://127.0.0.1:1/never-sent", "whsec_claim_test", &[])
        .await
        .expect("endpoint");

    let mut ids = Vec::new();
    for i in 0..n {
        let accepted = store
            .insert_event_and_fan_out("claim.test", format!(r#"{{"n":{i}}}"#).as_bytes())
            .await
            .expect("insert");
        ids.extend(accepted.delivery_ids);
    }
    ids
}

#[sqlx::test(migrations = "./migrations")]
async fn concurrent_workers_never_claim_the_same_delivery(pool: PgPool) {
    let store = Store::from_pool(pool);
    let seeded = seed(&store, 40).await;

    // Eight workers claiming at once, with batch sizes larger than a fair share so
    // they genuinely compete for the same rows rather than tidily dividing them.
    let mut tasks = tokio::task::JoinSet::new();
    for w in 0..8 {
        let s = store.clone();
        tasks.spawn(async move { s.claim_batch(10, &format!("worker-{w}")).await.unwrap() });
    }

    let mut claimed: Vec<Uuid> = Vec::new();
    while let Some(res) = tasks.join_next().await {
        claimed.extend(res.unwrap().into_iter().map(|d| d.delivery_id));
    }

    let mut unique = claimed.clone();
    unique.sort();
    unique.dedup();

    assert_eq!(
        unique.len(),
        claimed.len(),
        "the same delivery was handed to more than one worker"
    );
    assert_eq!(
        claimed.len(),
        seeded.len(),
        "eight workers claiming ten each should between them take all forty"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn a_claimed_delivery_is_not_claimable_again(pool: PgPool) {
    let store = Store::from_pool(pool);
    seed(&store, 3).await;

    let first = store.claim_batch(10, "worker-a").await.unwrap();
    assert_eq!(first.len(), 3);

    let second = store.claim_batch(10, "worker-b").await.unwrap();
    assert!(
        second.is_empty(),
        "already-claimed deliveries were handed out a second time"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn a_released_delivery_becomes_claimable_again(pool: PgPool) {
    let store = Store::from_pool(pool);
    let id = seed(&store, 1).await[0];

    assert_eq!(store.claim_batch(10, "worker-a").await.unwrap().len(), 1);

    // A worker shutting down puts back whatever it did not finish.
    store.release(id).await.unwrap();

    let again = store.claim_batch(10, "worker-b").await.unwrap();
    assert_eq!(
        again.len(),
        1,
        "a released delivery must return to the queue"
    );

    // Releasing must not charge an attempt: nothing was tried. Getting this wrong
    // lets shutdowns silently eat a delivery's retry budget.
    let d = store.get_delivery(id).await.unwrap().unwrap();
    assert_eq!(d.attempt, 0, "release must not consume an attempt");
}

#[sqlx::test(migrations = "./migrations")]
async fn claim_batch_respects_its_limit(pool: PgPool) {
    let store = Store::from_pool(pool);
    seed(&store, 10).await;

    let batch = store.claim_batch(4, "worker-a").await.unwrap();
    assert_eq!(batch.len(), 4, "claim_batch must honour its limit exactly");

    let rest = store.claim_batch(100, "worker-b").await.unwrap();
    assert_eq!(
        rest.len(),
        6,
        "the remaining rows should still be claimable"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn claiming_an_empty_queue_returns_nothing(pool: PgPool) {
    let store = Store::from_pool(pool);
    assert!(store.claim_batch(10, "worker-a").await.unwrap().is_empty());
}

#[sqlx::test(migrations = "./migrations")]
async fn the_claim_uses_the_partial_index_rather_than_scanning(pool: PgPool) {
    let store = Store::from_pool(pool);
    seed(&store, 5).await;

    // Everything below has to happen on one connection. `SET` configures the
    // session, and a pool hands out whichever connection is free, so setting on the
    // pool and explaining on the pool can easily be two different sessions — the
    // setting applies to one and the plan comes from the other.
    let mut conn = store.pool().acquire().await.unwrap();

    // Postgres will happily sequential-scan a tiny table, and it is right to: five
    // rows are cheaper to read whole than to look up. ANALYZE plus disabling seqscan
    // turns the question into "*can* this query use the index", which is the part
    // that governs behaviour once the table holds real history.
    sqlx::query("ANALYZE deliveries")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("SET enable_seqscan = off")
        .execute(&mut *conn)
        .await
        .unwrap();

    // Explains the text the claim itself runs, not a copy of it.
    let plan: Vec<String> = sqlx::query_scalar(relay_store::EXPLAIN_CLAIM_CANDIDATES_SQL)
        .bind(10i64)
        .fetch_all(&mut *conn)
        .await
        .unwrap();
    let plan = plan.join("\n");

    assert!(
        plan.contains("deliveries_pending_due_idx"),
        "the claim must be able to use the partial index on pending rows.\n\
         A sequential scan is fine on an empty table and stalls the entire system \
         once the table holds real history.\nPlan was:\n{plan}"
    );
}

#[sqlx::test(migrations = "./migrations")]
async fn a_history_page_seeks_rather_than_scans(pool: PgPool) {
    let store = Store::from_pool(pool);
    let endpoint = store
        .create_endpoint("https://example.com/hook", "whsec_history_plan", &[])
        .await
        .expect("endpoint");
    for i in 0..5 {
        store
            .insert_event_and_fan_out("order.paid", format!(r#"{{"n":{i}}}"#).as_bytes())
            .await
            .expect("insert");
    }

    // Same reasoning as the claim's plan test above: Postgres is right to scan five
    // rows, so the question asked is whether the index is *reachable*, which is what
    // governs behaviour once this table holds a year of history.
    let mut conn = store.pool().acquire().await.unwrap();
    sqlx::query("ANALYZE deliveries")
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("SET enable_seqscan = off")
        .execute(&mut *conn)
        .await
        .unwrap();

    let plan: Vec<String> = sqlx::query_scalar(relay_store::EXPLAIN_DELIVERY_PAGE_SQL)
        .bind(endpoint.id)
        .bind(None::<String>)
        .bind(None::<chrono::DateTime<chrono::Utc>>)
        .bind(None::<uuid::Uuid>)
        .bind(10i64)
        .fetch_all(&mut *conn)
        .await
        .unwrap();
    let plan = plan.join("\n");

    assert!(
        plan.contains("deliveries_endpoint_created_id_idx"),
        "the history page must be able to seek on (endpoint_id, created_at, id).\n\
         Without it every page re-reads the endpoint's whole history, which is the \
         cost `OFFSET` was avoided to escape.\nPlan was:\n{plan}"
    );
    // The point of paging by position: the rows come back already ordered, so the
    // page is the first N of an index walk rather than a sort of everything.
    assert!(
        !plan.contains("Sort"),
        "the page should be read in index order, not sorted.\nPlan was:\n{plan}"
    );
}
