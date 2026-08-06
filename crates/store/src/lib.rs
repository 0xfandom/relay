//! Postgres persistence for Relay.
//!
//! Everything that touches the database lives here, behind plain methods, so the
//! API and dispatcher crates never write SQL of their own.
//!
//! Queries are written with `sqlx::query_as` rather than the `query!` macro. The
//! macro checks SQL against a live database *at compile time*, which is excellent
//! but makes `cargo build` depend on a running Postgres (or a checked-in offline
//! cache). That trade is worth making later; for now the build stays hermetic.

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

    /// Apply any migrations that have not run yet. Safe to call on every start:
    /// sqlx records which have been applied.
    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
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

    pub async fn mark_succeeded(&self, delivery_id: Uuid) -> Result<()> {
        sqlx::query(
            "UPDATE deliveries
             SET status = 'succeeded', attempt = attempt + 1, locked_at = NULL, locked_by = NULL
             WHERE id = $1",
        )
        .bind(delivery_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// M1 has no retry policy yet, so any failure is terminal. M3 replaces this
    /// with a classifier and a backoff schedule.
    pub async fn mark_dead(&self, delivery_id: Uuid) -> Result<()> {
        sqlx::query(
            "UPDATE deliveries
             SET status = 'dead', attempt = attempt + 1, locked_at = NULL, locked_by = NULL
             WHERE id = $1",
        )
        .bind(delivery_id)
        .execute(&self.pool)
        .await?;
        Ok(())
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

    /// Append one attempt row. Never updated afterwards — this table is the audit
    /// ledger, and rewriting history would defeat its purpose.
    #[allow(clippy::too_many_arguments)]
    pub async fn record_attempt(
        &self,
        delivery_id: Uuid,
        attempt_no: i32,
        http_status: Option<i32>,
        latency_ms: i32,
        outcome_class: &str,
        error: Option<&str>,
        response_snippet: Option<&str>,
    ) -> Result<()> {
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
        .execute(&self.pool)
        .await?;
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
