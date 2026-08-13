//! Postgres persistence for Relay.
//!
//! Everything that touches the database lives here, behind plain methods, so the
//! API and dispatcher crates never write SQL of their own.
//!
//! Queries are written with `sqlx::query_as` rather than the `query!` macro. The
//! macro checks SQL against a live database *at compile time*, which is excellent
//! but makes `cargo build` depend on a running Postgres (or a checked-in offline
//! cache). That trade is worth making later; for now the build stays hermetic.

use std::time::Duration;

use serde::Serialize;
use sqlx::postgres::{PgPool, PgPoolOptions};
use uuid::Uuid;

pub mod models;

pub use models::{Delivery, Endpoint, PendingDelivery};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("migration failed: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("endpoint not found")]
    EndpointNotFound,
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// Selects the deliveries a worker is allowed to take, soonest deadline first.
/// `$1` is the batch size.
///
/// A macro rather than a `const` so the text is a literal, which lets `concat!`
/// paste it into the full claim below at compile time. One definition, two users:
/// the claim that runs it, and the test that asks Postgres to `EXPLAIN` it. A test
/// that explains a hand-copied lookalike proves nothing — the copy keeps its index
/// while the real query quietly loses one.
macro_rules! claim_candidates_sql {
    () => {
        "SELECT id FROM deliveries
         WHERE status = 'pending' AND next_attempt_at <= now()
         ORDER BY next_attempt_at
         FOR UPDATE SKIP LOCKED
         LIMIT $1"
    };
}

/// The claim's candidate selection, wrapped in `EXPLAIN`. Exposed for the test that
/// asserts the partial index is reachable.
pub const EXPLAIN_CLAIM_CANDIDATES_SQL: &str = concat!("EXPLAIN ", claim_candidates_sql!());

const CLAIM_BATCH_SQL: &str = concat!(
    "WITH claimed AS (
         UPDATE deliveries
         SET status = 'inflight', locked_at = now(), locked_by = $2
         WHERE id IN (",
    claim_candidates_sql!(),
    ")
         RETURNING id, attempt, event_id, endpoint_id
     )
     SELECT c.id          AS delivery_id,
            c.attempt     AS attempt,
            e.event_type  AS event_type,
            e.raw_payload AS raw_payload,
            ep.id         AS endpoint_id,
            ep.url        AS url,
            ep.secret     AS secret
     FROM claimed c
     JOIN events    e  ON e.id  = c.event_id
     JOIN endpoints ep ON ep.id = c.endpoint_id"
);

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    /// Open a connection pool. `max_connections` bounds how much of Postgres this
    /// process can occupy — important later, when a hung endpoint must not be able
    /// to starve the ingest path of connections.
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    /// Wrap a pool that already exists.
    ///
    /// Used by `#[sqlx::test]`, which hands each test its own freshly migrated
    /// database. Sharing one database between tests means they claim each other's
    /// work and fail for reasons that have nothing to do with the code.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Apply any migrations that have not run yet. Safe to call on every start:
    /// sqlx records which have been applied.
    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Cheapest possible round trip, for readiness checks.
    pub async fn ping(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    /// Take ownership of a delivery before sending it.
    ///
    /// This is what stops the send loop from re-sending the same delivery when a
    /// later write fails: once the row is `inflight` it is no longer `pending`, so
    /// the loop will not pick it up again on the next pass.
    ///
    /// The trade is that a crash now strands the row in `inflight` until something
    /// releases it. A stranded delivery is strictly better than an endpoint being
    /// hammered with duplicates, and the reaper that releases expired claims
    /// arrives in M2 alongside the lease.
    ///
    /// Returns false if the row was not claimable, which today means another pass
    /// already took it.
    pub async fn claim(&self, delivery_id: Uuid, worker: &str) -> Result<bool> {
        let claimed = sqlx::query(
            "UPDATE deliveries
             SET status = 'inflight', locked_at = now(), locked_by = $2
             WHERE id = $1 AND status = 'pending'",
        )
        .bind(delivery_id)
        .bind(worker)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(claimed == 1)
    }

    /// Claim a batch of due deliveries for exclusive processing.
    ///
    /// This is the query the whole dispatcher is built around, and the interesting
    /// part is `FOR UPDATE SKIP LOCKED`.
    ///
    /// `FOR UPDATE` alone locks the rows the inner select touches. With several
    /// workers running the same query at the same instant, the second worker blocks
    /// on the first one's locks, the third blocks behind the second, and a pool of
    /// eight workers behaves like one worker with a queue behind it. Adding
    /// `SKIP LOCKED` tells Postgres to pass over any row another transaction is
    /// already holding and take the next free one instead, so each worker walks
    /// away with a disjoint set and none of them ever waits.
    ///
    /// The inner `SELECT` is separate from the `UPDATE` on purpose: `LIMIT` and
    /// `FOR UPDATE SKIP LOCKED` belong to a select, and this shape lets Postgres
    /// use the partial index on pending rows to find candidates before locking
    /// anything.
    ///
    /// The surrounding CTE returns the claimed rows joined with the payload and the
    /// endpoint, so claiming a batch costs one round trip rather than one plus N.
    pub async fn claim_batch(&self, limit: i64, worker: &str) -> Result<Vec<PendingDelivery>> {
        let rows = sqlx::query_as::<_, PendingDelivery>(CLAIM_BATCH_SQL)
            .bind(limit)
            .bind(worker)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    /// Return a claimed delivery to the queue without consuming an attempt.
    ///
    /// Used when a worker is shutting down and will not finish what it holds. The
    /// attempt counter is untouched: nothing was tried, so nothing should be
    /// charged against the delivery's retry budget.
    pub async fn release(&self, delivery_id: Uuid) -> Result<()> {
        sqlx::query(
            "UPDATE deliveries
             SET status = 'pending', locked_at = NULL, locked_by = NULL
             WHERE id = $1 AND status = 'inflight'",
        )
        .bind(delivery_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Return deliveries whose lease has expired to the pending pool.
    ///
    /// A worker that dies mid-send — killed, out of memory, host lost — leaves its
    /// rows `inflight` with nobody holding them. They are not `pending`, so the
    /// claim query steps over them, and they would sit there undelivered forever
    /// with nothing reporting a problem. This is the only thing that finds them.
    ///
    /// The attempt counter is untouched. Whether the request actually reached the
    /// endpoint is unknown, and charging a retry for an attempt that may never have
    /// happened spends the delivery's budget on a guess.
    ///
    /// `lease_ttl` must exceed the sender's total request timeout. A shorter one
    /// rescues deliveries that are still legitimately in flight, and the endpoint
    /// receives them twice.
    ///
    /// Returns how many were rescued. Zero is the expected value; anything else
    /// means workers are dying.
    pub async fn reap_expired_leases(&self, lease_ttl: Duration) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE deliveries
             SET status = 'pending', locked_at = NULL, locked_by = NULL
             WHERE status = 'inflight'
               AND locked_at < now() - make_interval(secs => $1)",
        )
        .bind(lease_ttl.as_secs_f64())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    // ---------------------------------------------------------------- endpoints

    pub async fn create_endpoint(
        &self,
        url: &str,
        secret: &str,
        event_types: &[String],
    ) -> Result<Endpoint> {
        let ep = sqlx::query_as::<_, Endpoint>(
            "INSERT INTO endpoints (url, secret, event_types)
             VALUES ($1, $2, $3)
             RETURNING id, url, secret, event_types, enabled",
        )
        .bind(url)
        .bind(secret)
        .bind(event_types)
        .fetch_one(&self.pool)
        .await?;
        Ok(ep)
    }

    pub async fn get_endpoint(&self, id: Uuid) -> Result<Endpoint> {
        sqlx::query_as::<_, Endpoint>(
            "SELECT id, url, secret, event_types, enabled FROM endpoints WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or(StoreError::EndpointNotFound)
    }

    // ------------------------------------------------------------------- events

    /// Persist an event and fan it out to every subscribed endpoint, in one
    /// transaction.
    ///
    /// Both halves must commit together. An event with no deliveries is silently
    /// lost; deliveries with no event violate the foreign key. The transaction is
    /// what makes "accepted" mean something.
    pub async fn insert_event_and_fan_out(
        &self,
        event_type: &str,
        raw_payload: &[u8],
    ) -> Result<Accepted> {
        let mut tx = self.pool.begin().await?;

        let event_id: Uuid = sqlx::query_scalar(
            "INSERT INTO events (event_type, raw_payload) VALUES ($1, $2) RETURNING id",
        )
        .bind(event_type)
        .bind(raw_payload)
        .fetch_one(&mut *tx)
        .await?;

        // An empty `event_types` array means "subscribe to everything", which keeps
        // the common case free of configuration.
        let delivery_ids: Vec<Uuid> = sqlx::query_scalar(
            "INSERT INTO deliveries (event_id, endpoint_id)
             SELECT $1, id FROM endpoints
             WHERE enabled
               AND (cardinality(event_types) = 0 OR $2 = ANY(event_types))
             RETURNING id",
        )
        .bind(event_id)
        .bind(event_type)
        .fetch_all(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(Accepted {
            event_id,
            delivery_ids,
        })
    }

    // --------------------------------------------------------------- deliveries

    /// The next delivery that is due, joined with everything the sender needs.
    ///
    /// Deliberately simple for M1: one row, no locking, single sender. M2 replaces
    /// this with a `FOR UPDATE SKIP LOCKED` batch claim and a lease.
    pub async fn next_pending_delivery(&self) -> Result<Option<PendingDelivery>> {
        let row = sqlx::query_as::<_, PendingDelivery>(
            "SELECT d.id            AS delivery_id,
                    d.attempt       AS attempt,
                    e.event_type    AS event_type,
                    e.raw_payload   AS raw_payload,
                    ep.id           AS endpoint_id,
                    ep.url          AS url,
                    ep.secret       AS secret
             FROM deliveries d
             JOIN events    e  ON e.id  = d.event_id
             JOIN endpoints ep ON ep.id = d.endpoint_id
             WHERE d.status = 'pending' AND d.next_attempt_at <= now()
             ORDER BY d.next_attempt_at
             LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// One specific delivery, regardless of due time.
    ///
    /// Tests use this so that concurrently running tests do not steal each other's
    /// work out of a shared queue.
    pub async fn pending_delivery_by_id(&self, id: Uuid) -> Result<Option<PendingDelivery>> {
        let row = sqlx::query_as::<_, PendingDelivery>(
            "SELECT d.id            AS delivery_id,
                    d.attempt       AS attempt,
                    e.event_type    AS event_type,
                    e.raw_payload   AS raw_payload,
                    ep.id           AS endpoint_id,
                    ep.url          AS url,
                    ep.secret       AS secret
             FROM deliveries d
             JOIN events    e  ON e.id  = d.event_id
             JOIN endpoints ep ON ep.id = d.endpoint_id
             WHERE d.id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn get_delivery(&self, id: Uuid) -> Result<Option<Delivery>> {
        let d = sqlx::query_as::<_, Delivery>(
            "SELECT id, event_id, endpoint_id, status, attempt FROM deliveries WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(d)
    }

    // ------------------------------------------------------------------ attempts

    /// Append the attempt row and move the delivery to its final state, together.
    ///
    /// One transaction, because these two writes describe the same fact. Landing
    /// the attempt without the status change would leave a delivery that looks
    /// unfinished but has already been sent; landing the status change without the
    /// attempt would claim an outcome with no evidence for it.
    ///
    /// The attempt row is inserted first so that, within the transaction, the
    /// evidence precedes the conclusion.
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_attempt(
        &self,
        delivery_id: Uuid,
        attempt_no: i32,
        final_status: DeliveryStatus,
        http_status: Option<i32>,
        latency_ms: i32,
        outcome_class: &str,
        error: Option<&str>,
        response_snippet: Option<&str>,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            "INSERT INTO delivery_attempts
                 (delivery_id, attempt_no, http_status, latency_ms, outcome_class, error, response_snippet)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(delivery_id)
        .bind(attempt_no)
        .bind(http_status)
        .bind(latency_ms)
        .bind(outcome_class)
        .bind(error)
        .bind(response_snippet)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            "UPDATE deliveries
             SET status = $2, attempt = attempt + 1, locked_at = NULL, locked_by = NULL
             WHERE id = $1",
        )
        .bind(delivery_id)
        .bind(final_status.as_str())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn attempts_for(&self, delivery_id: Uuid) -> Result<i64> {
        let n: i64 =
            sqlx::query_scalar("SELECT count(*) FROM delivery_attempts WHERE delivery_id = $1")
                .bind(delivery_id)
                .fetch_one(&self.pool)
                .await?;
        Ok(n)
    }
}

#[derive(Debug, Serialize)]
pub struct Accepted {
    pub event_id: Uuid,
    pub delivery_ids: Vec<Uuid>,
}

/// The states a delivery can finish an attempt in.
///
/// A typed value rather than a bare string, so a typo is a compile error instead
/// of a row that silently violates the table's CHECK constraint at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    Succeeded,
    Dead,
}

impl DeliveryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Dead => "dead",
        }
    }
}
