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

use relay_domain::{
    breaker::{self, Event, Health, Policy as BreakerPolicy, State as BreakerState},
    rate_limit::Rate,
};
use serde::Serialize;
use sqlx::postgres::{PgPool, PgPoolOptions};
use uuid::Uuid;

pub mod models;

pub use models::{Attempt, BreakerRow, DeadLetter, Delivery, Endpoint, PendingDelivery};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("migration failed: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("endpoint not found")]
    EndpointNotFound,
    /// The key exists but was first used for a different event type or body.
    /// A caller bug, not a race — and reported rather than swallowed, because the
    /// alternative is answering the second request with the first one's result and
    /// losing an event while looking successful.
    #[error("idempotency key was already used for a different request")]
    IdempotencyKeyReused,
    /// Every attempt to claim a key both failed to insert and failed to find the
    /// winner. Should be unreachable; kept because the alternative to giving up is
    /// spinning forever.
    #[error("could not resolve a concurrent request with the same idempotency key")]
    IdempotencyRaceUnresolved,
    #[error("could not encode response: {0}")]
    Encode(serde_json::Error),
}

/// Whether an error is Postgres refusing a duplicate key.
///
/// `23505` is `unique_violation`. Matched on the SQLSTATE rather than the message
/// text, which is localised and not a stable interface.
fn is_unique_violation(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if db.code().as_deref() == Some("23505"))
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
            ep.secret     AS secret,
            ep.rate_per_second AS rate_per_second,
            ep.burst      AS burst,
            ep.breaker_state AS breaker_state,
            ep.breaker_probe_at AS breaker_probe_at
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

    /// Change how fast an endpoint may be sent to.
    ///
    /// Separate from creation so that the common case needs no configuration: an
    /// endpoint starts at the schema's conservative default and is raised once
    /// someone knows what the destination can take.
    pub async fn set_endpoint_rate(&self, endpoint_id: Uuid, rate: Rate) -> Result<()> {
        let result =
            sqlx::query("UPDATE endpoints SET rate_per_second = $2, burst = $3 WHERE id = $1")
                .bind(endpoint_id)
                .bind(rate.per_second)
                .bind(rate.burst)
                .execute(&self.pool)
                .await?;
        if result.rows_affected() == 0 {
            return Err(StoreError::EndpointNotFound);
        }
        Ok(())
    }

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
        let (event_id, delivery_ids) =
            Self::insert_event_tx(&mut tx, event_type, raw_payload).await?;
        tx.commit().await?;

        Ok(Accepted {
            event_id,
            delivery_ids,
        })
    }

    /// The event insert and its fan-out, without the transaction boundary.
    ///
    /// Factored out because the idempotent path needs the same two writes to share a
    /// transaction with the key claim — and needs to be able to roll all three back
    /// together when it loses the race.
    async fn insert_event_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        event_type: &str,
        raw_payload: &[u8],
    ) -> Result<(Uuid, Vec<Uuid>)> {
        let event_id: Uuid = sqlx::query_scalar(
            "INSERT INTO events (event_type, raw_payload) VALUES ($1, $2) RETURNING id",
        )
        .bind(event_type)
        .bind(raw_payload)
        .fetch_one(&mut **tx)
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
        .fetch_all(&mut **tx)
        .await?;

        Ok((event_id, delivery_ids))
    }

    // ---------------------------------------------------------- idempotent ingest

    /// How many times to re-resolve a lost race before giving up.
    ///
    /// One pass is normally enough: losing the unique-index race means the winner
    /// has committed, so the next lookup finds it. A second pass only matters if the
    /// winner's key was pruned in the microseconds in between, and a third exists so
    /// that a pathological interleaving terminates rather than spinning.
    const IDEMPOTENCY_ATTEMPTS: usize = 3;

    /// Ingest an event at most once for a given key.
    ///
    /// The mechanism is the unique index on `idempotency_keys.key`, and the order of
    /// operations is what makes it correct: the event and its deliveries are
    /// inserted first, in the same transaction as the key. If the key insert loses,
    /// the whole transaction rolls back and the event never existed — no orphan, no
    /// half-fanned-out delivery set, no compensating cleanup to get wrong.
    ///
    /// The interesting case is two identical requests arriving at once. Both find no
    /// key, both do the work, both try to insert. Postgres blocks the second insert
    /// until the first transaction resolves rather than failing it immediately, so
    /// the loser learns the truth instead of guessing at it:
    ///
    /// - winner commits → loser gets a unique violation, rolls back, reads the
    ///   winner's row and returns it
    /// - winner rolls back → loser's insert succeeds and it becomes the winner
    ///
    /// A unique violation is therefore not an error to report. Surfacing it as a
    /// `5xx` would be the worst possible answer: the caller would retry, hit the same
    /// race, and get the same `5xx` forever.
    ///
    /// Returns [`StoreError::IdempotencyKeyReused`] if the key exists but was first
    /// used for a different request.
    pub async fn insert_event_idempotent(
        &self,
        event_type: &str,
        raw_payload: &[u8],
        key: &str,
        request_digest: &[u8],
    ) -> Result<Ingested> {
        for _ in 0..Self::IDEMPOTENCY_ATTEMPTS {
            // Checked before doing any work, so the ordinary duplicate — a retry
            // arriving well after the original — costs one indexed lookup rather
            // than an event insert, a fan-out and a rollback.
            if let Some(found) = self.find_idempotent(key, request_digest).await? {
                return Ok(found);
            }

            let mut tx = self.pool.begin().await?;
            let (event_id, delivery_ids) =
                Self::insert_event_tx(&mut tx, event_type, raw_payload).await?;
            let accepted = Accepted {
                event_id,
                delivery_ids,
            };
            // Serialised here rather than by the caller so that the bytes stored and
            // the bytes returned are the same object, not two renderings that happen
            // to agree today.
            let response = serde_json::to_vec(&accepted).map_err(StoreError::Encode)?;

            let claimed = sqlx::query(
                "INSERT INTO idempotency_keys (key, event_id, request_digest, response)
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(key)
            .bind(event_id)
            .bind(request_digest)
            .bind(&response)
            .execute(&mut *tx)
            .await;

            match claimed {
                Ok(_) => {
                    tx.commit().await?;
                    return Ok(Ingested {
                        event_id,
                        response,
                        replayed: false,
                    });
                }
                Err(e) if is_unique_violation(&e) => {
                    // Undoes the event and every delivery row with it. This is the
                    // whole reason the key insert shares their transaction.
                    tx.rollback().await?;
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(StoreError::IdempotencyRaceUnresolved)
    }

    /// The stored result for a key, if it has one and the request matches.
    async fn find_idempotent(&self, key: &str, request_digest: &[u8]) -> Result<Option<Ingested>> {
        let row: Option<(Uuid, Vec<u8>, Vec<u8>)> = sqlx::query_as(
            "SELECT event_id, request_digest, response FROM idempotency_keys WHERE key = $1",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            None => Ok(None),
            Some((event_id, stored_digest, response)) => {
                if stored_digest != request_digest {
                    return Err(StoreError::IdempotencyKeyReused);
                }
                Ok(Some(Ingested {
                    event_id,
                    response,
                    replayed: true,
                }))
            }
        }
    }

    /// Delete keys older than the retention window.
    ///
    /// Keys are only useful for as long as a producer might still retry, and that is
    /// minutes. Keeping them permanently would grow this table as fast as the event
    /// table to answer a question nobody asks after the first hour.
    pub async fn prune_idempotency_keys(&self, older_than: Duration) -> Result<u64> {
        let result = sqlx::query(
            "DELETE FROM idempotency_keys WHERE created_at < now() - make_interval(secs => $1)",
        )
        .bind(older_than.as_secs_f64())
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
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
                    ep.secret       AS secret,
                    ep.rate_per_second AS rate_per_second,
                    ep.burst        AS burst,
                    ep.breaker_state AS breaker_state,
                    ep.breaker_probe_at AS breaker_probe_at
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
                    ep.secret       AS secret,
                    ep.rate_per_second AS rate_per_second,
                    ep.burst        AS burst,
                    ep.breaker_state AS breaker_state,
                    ep.breaker_probe_at AS breaker_probe_at
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
            "SELECT id, event_id, endpoint_id, status, attempt, dead_reason, generation
             FROM deliveries WHERE id = $1",
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
        result: AttemptResult,
        http_status: Option<i32>,
        latency_ms: i32,
        outcome_class: &str,
        error: Option<&str>,
        response_snippet: Option<&str>,
        worker_id: &str,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        // `next_attempt_at` is written here as well as on the delivery. The
        // deliveries table only ever holds the *latest* schedule, so without a copy
        // per attempt the earlier ones are overwritten and the backoff cannot be
        // audited after the fact.
        sqlx::query(
            "INSERT INTO delivery_attempts
                 (delivery_id, attempt_no, http_status, latency_ms, outcome_class,
                  error, response_snippet, worker_id, next_attempt_at,
                  generation)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                     CASE WHEN $9::double precision IS NULL THEN NULL
                          ELSE now() + make_interval(secs => $9) END,
                     (SELECT generation FROM deliveries WHERE id = $1))",
        )
        .bind(delivery_id)
        .bind(attempt_no)
        .bind(http_status)
        .bind(latency_ms)
        .bind(outcome_class)
        .bind(error)
        .bind(response_snippet)
        .bind(worker_id)
        .bind(result.retry_delay().map(|d| d.as_secs_f64()))
        .execute(&mut *tx)
        .await?;

        // `next_attempt_at` moves only on a retry. On a terminal outcome the row is
        // no longer `pending`, so the claim query ignores the column entirely and
        // leaving it alone keeps the last scheduled time visible for debugging.
        sqlx::query(
            "UPDATE deliveries
             SET status = $2,
                 attempt = attempt + 1,
                 locked_at = NULL,
                 locked_by = NULL,
                 dead_reason = $4,
                 next_attempt_at = CASE
                     WHEN $3::double precision IS NULL THEN next_attempt_at
                     ELSE now() + make_interval(secs => $3)
                 END
             WHERE id = $1",
        )
        .bind(delivery_id)
        .bind(result.status().as_str())
        .bind(result.retry_delay().map(|d| d.as_secs_f64()))
        .bind(result.dead_reason().map(|r| r.as_str()))
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// Put a claimed delivery back without spending an attempt.
    ///
    /// The distinction from [`Store::finish_attempt`] is the whole point: a deferral
    /// is not a failure. Nothing was sent, the endpoint was never asked, and there is
    /// no information about whether it would have worked. Charging an attempt for it
    /// would mean a busy endpoint's deliveries reach the dead letter queue having
    /// never had a single request made to them — a retry budget spent entirely on our
    /// own throttle.
    ///
    /// So `attempt` is left alone and the row goes back to `pending`, due when the
    /// caller says. The attempt log still gets a row, because "we held this back for
    /// 300ms" is exactly the kind of thing someone asking why a webhook was late
    /// needs to see.
    pub async fn defer_delivery(
        &self,
        delivery_id: Uuid,
        attempt_no: i32,
        delay: Duration,
        reason: &str,
        worker_id: &str,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            "INSERT INTO delivery_attempts
                 (delivery_id, attempt_no, http_status, latency_ms, outcome_class,
                  error, response_snippet, worker_id, next_attempt_at, generation)
             VALUES ($1, $2, NULL, 0, 'deferred', $3, NULL, $4,
                     now() + make_interval(secs => $5),
                     (SELECT generation FROM deliveries WHERE id = $1))",
        )
        .bind(delivery_id)
        .bind(attempt_no)
        .bind(reason)
        .bind(worker_id)
        .bind(delay.as_secs_f64())
        .execute(&mut *tx)
        .await?;

        // Note the absent `attempt = attempt + 1`.
        sqlx::query(
            "UPDATE deliveries
             SET status = 'pending',
                 locked_at = NULL,
                 locked_by = NULL,
                 next_attempt_at = now() + make_interval(secs => $2)
             WHERE id = $1",
        )
        .bind(delivery_id)
        .bind(delay.as_secs_f64())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    // ------------------------------------------------------------------ breaker

    /// The breaker as stored, for tests and for the admin surface M7 will want.
    pub async fn endpoint_breaker(&self, endpoint_id: Uuid) -> Result<BreakerRow> {
        let row = sqlx::query_as::<_, BreakerRow>(
            "SELECT breaker_state, consecutive_failures, breaker_trips,
                    breaker_probe_at, breaker_opened_at
             FROM endpoints WHERE id = $1",
        )
        .bind(endpoint_id)
        .fetch_optional(&self.pool)
        .await?;
        row.ok_or(StoreError::EndpointNotFound)
    }

    /// Fold one attempt's evidence into the endpoint's breaker.
    ///
    /// Read and write in one transaction with the row locked, because the decision
    /// depends on what is already there: two workers reporting a failure at the same
    /// instant against an unlocked row would both read four consecutive failures,
    /// both write five, and the breaker would record one failure where two happened.
    /// At a threshold of five that is the difference between tripping and not.
    ///
    /// This is the reason the state lives in Postgres at all. Held in process memory
    /// it looks correct with one worker and silently fails with several: each sees a
    /// fraction of the failures, none reaches the threshold, and every worker
    /// independently concludes the endpoint is merely unlucky.
    ///
    /// Returns the state the breaker is now in.
    pub async fn record_health(
        &self,
        endpoint_id: Uuid,
        health: Health,
        policy: &BreakerPolicy,
    ) -> Result<BreakerState> {
        let mut tx = self.pool.begin().await?;

        let current = sqlx::query_as::<_, BreakerRow>(
            "SELECT breaker_state, consecutive_failures, breaker_trips,
                    breaker_probe_at, breaker_opened_at
             FROM endpoints WHERE id = $1
             FOR UPDATE",
        )
        .bind(endpoint_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StoreError::EndpointNotFound)?;

        // The decision itself is the pure function in `relay_domain::breaker`. None
        // of it is expressed in SQL, so the rules are tested exhaustively in
        // microseconds and this method only has to store the answer.
        let next = breaker::transition(current.breaker(), Event::Attempted(health), policy);

        sqlx::query(
            "UPDATE endpoints
             SET breaker_state = $2,
                 consecutive_failures = $3,
                 breaker_trips = $4,
                 breaker_probe_at = CASE
                     WHEN $5::double precision IS NOT NULL
                         THEN now() + make_interval(secs => $5)
                     WHEN $2 = 'closed' THEN NULL
                     ELSE breaker_probe_at
                 END,
                 breaker_opened_at = CASE
                     WHEN $5::double precision IS NOT NULL THEN now()
                     WHEN $2 = 'closed' THEN NULL
                     ELSE breaker_opened_at
                 END
             WHERE id = $1",
        )
        .bind(endpoint_id)
        .bind(next.breaker.state.as_str())
        .bind(next.breaker.consecutive_failures as i32)
        .bind(next.breaker.trips as i32)
        .bind(next.cooldown.map(|d| d.as_secs_f64()))
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(next.breaker.state)
    }

    /// Move an open breaker whose cooldown has expired to half-open.
    ///
    /// Deliberately unconditional for now: several workers arriving at once will all
    /// see the cooldown expired and all be let through, which rushes an endpoint that
    /// has only just come back. #20 replaces this with a single conditional
    /// `UPDATE ... RETURNING` so the database picks one winner.
    pub async fn set_breaker_half_open(&self, endpoint_id: Uuid) -> Result<()> {
        sqlx::query(
            "UPDATE endpoints
             SET breaker_state = 'half_open'
             WHERE id = $1 AND breaker_state = 'open' AND breaker_probe_at <= now()",
        )
        .bind(endpoint_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Force a breaker back to closed. For tests and for an operator who knows the
    /// endpoint is fine and does not want to wait out the cooldown.
    pub async fn reset_breaker(&self, endpoint_id: Uuid) -> Result<()> {
        sqlx::query(
            "UPDATE endpoints
             SET breaker_state = 'closed', consecutive_failures = 0, breaker_trips = 0,
                 breaker_probe_at = NULL, breaker_opened_at = NULL
             WHERE id = $1",
        )
        .bind(endpoint_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // -------------------------------------------------------------- dead letters

    /// List parked deliveries, newest first.
    ///
    /// The filters are all optional and all bound rather than interpolated, so one
    /// query plan serves every combination: a `NULL` parameter simply disables its
    /// clause.
    pub async fn dead_letters(
        &self,
        filter: &DeadLetterFilter,
        limit: i64,
    ) -> Result<Vec<DeadLetter>> {
        let rows = sqlx::query_as::<_, DeadLetter>(
            "SELECT d.id            AS delivery_id,
                    d.endpoint_id   AS endpoint_id,
                    d.event_id      AS event_id,
                    e.event_type    AS event_type,
                    ep.url          AS url,
                    d.attempt       AS attempt,
                    d.generation    AS generation,
                    d.dead_reason   AS dead_reason,
                    d.created_at    AS created_at
             FROM deliveries d
             JOIN events    e  ON e.id  = d.event_id
             JOIN endpoints ep ON ep.id = d.endpoint_id
             WHERE d.status = 'dead'
               AND ($1::uuid IS NULL OR d.endpoint_id = $1)
               AND ($2::text IS NULL OR d.dead_reason = $2)
               AND ($3::text IS NULL OR e.event_type  = $3)
             ORDER BY d.created_at DESC
             LIMIT $4",
        )
        .bind(filter.endpoint_id)
        .bind(filter.reason.map(|r| r.as_str()))
        .bind(filter.event_type.as_deref())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Put one dead delivery back in the queue.
    ///
    /// Returns false if it does not exist or is not dead. Replaying something that
    /// is merely slow would hand a second worker a delivery the first is still
    /// sending.
    pub async fn replay(&self, delivery_id: Uuid) -> Result<bool> {
        let n = sqlx::query(
            "UPDATE deliveries SET
                 status          = 'pending',
                 dead_reason     = NULL,
                 attempt         = 0,
                 generation      = generation + 1,
                 next_attempt_at = now(),
                 locked_at       = NULL,
                 locked_by       = NULL
             WHERE id = $1 AND status = 'dead'",
        )
        .bind(delivery_id)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n == 1)
    }

    /// Put a filtered set of dead deliveries back in the queue.
    ///
    /// `limit` is required rather than optional. An unfiltered replay of a queue
    /// holding a million deliveries would schedule all of them at once, aimed at an
    /// endpoint that has only just recovered — the exact flood the jittered backoff
    /// exists to prevent, delivered deliberately.
    pub async fn replay_many(&self, filter: &DeadLetterFilter, limit: i64) -> Result<u64> {
        let n = sqlx::query(
            "UPDATE deliveries SET
                 status          = 'pending',
                 dead_reason     = NULL,
                 attempt         = 0,
                 generation      = generation + 1,
                 next_attempt_at = now(),
                 locked_at       = NULL,
                 locked_by       = NULL
             WHERE id IN (
                 SELECT d.id FROM deliveries d
                 JOIN events e ON e.id = d.event_id
                 WHERE d.status = 'dead'
                   AND ($1::uuid IS NULL OR d.endpoint_id = $1)
                   AND ($2::text IS NULL OR d.dead_reason = $2)
                   AND ($3::text IS NULL OR e.event_type  = $3)
                 ORDER BY d.created_at
                 LIMIT $4
             )",
        )
        .bind(filter.endpoint_id)
        .bind(filter.reason.map(|r| r.as_str()))
        .bind(filter.event_type.as_deref())
        .bind(limit)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n)
    }

    /// Every attempt made on a delivery, oldest first.
    ///
    /// The whole history comes from this table alone — the deliveries row holds only
    /// the current state, so it cannot answer "what happened on the third try".
    pub async fn attempt_history(&self, delivery_id: Uuid) -> Result<Vec<Attempt>> {
        let rows = sqlx::query_as::<_, Attempt>(
            "SELECT delivery_id, attempt_no, http_status, latency_ms, outcome_class,
                    generation, error, response_snippet, worker_id, next_attempt_at, at
             FROM delivery_attempts
             WHERE delivery_id = $1
             ORDER BY generation, attempt_no",
        )
        .bind(delivery_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
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

/// The outcome of an ingest that carried an idempotency key.
///
/// `response` is the body to send back, and it is the same bytes whether the event
/// was created now or created an hour ago: stored verbatim on the first request and
/// handed back untouched on every duplicate. A caller comparing two responses byte
/// for byte gets equality, which is the property that makes a retry safe.
#[derive(Debug, Clone)]
pub struct Ingested {
    pub event_id: Uuid,
    pub response: Vec<u8>,
    /// True when this request was recognised as a duplicate and nothing new was
    /// created. Worth reporting to the caller: silently succeeding is correct, but
    /// silently succeeding *for a different reason* is worth being able to see.
    pub replayed: bool,
}

/// The states a delivery can finish an attempt in.
///
/// A typed value rather than a bare string, so a typo is a compile error instead
/// of a row that silently violates the table's CHECK constraint at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    Pending,
    Succeeded,
    Dead,
}

impl DeliveryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Succeeded => "succeeded",
            Self::Dead => "dead",
        }
    }
}

/// How an attempt ended, and what happens to the delivery next.
///
/// One value rather than a status plus an optional delay, because the two are not
/// independent: a retry without a delay would be claimed again immediately, and a
/// terminal outcome with one describes a schedule for a delivery that is finished.
/// Pairing them makes the invalid combinations unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AttemptResult {
    Succeeded,
    /// Back to `pending`, claimable again once the delay has passed.
    Retry {
        delay: Duration,
    },
    /// Given up on, for the recorded reason.
    Dead {
        reason: DeadReason,
    },
}

/// Why a delivery was given up on.
///
/// The two need completely different responses. A permanent failure means the URL
/// or the payload is wrong and someone has to change something. Exhausted attempts
/// mean the endpoint was down longer than the retry budget, and a replay once it is
/// back will probably just work. Recording only "dead" makes those indistinguishable
/// and the queue untriageable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadReason {
    /// The first attempt already showed it would never work.
    PermanentFailure,
    /// It might have worked, and we ran out of tries.
    AttemptsExhausted,
}

impl DeadReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PermanentFailure => "permanent_failure",
            Self::AttemptsExhausted => "attempts_exhausted",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "permanent_failure" => Some(Self::PermanentFailure),
            "attempts_exhausted" => Some(Self::AttemptsExhausted),
            _ => None,
        }
    }
}

impl AttemptResult {
    pub fn status(self) -> DeliveryStatus {
        match self {
            Self::Succeeded => DeliveryStatus::Succeeded,
            Self::Retry { .. } => DeliveryStatus::Pending,
            Self::Dead { .. } => DeliveryStatus::Dead,
        }
    }

    pub fn retry_delay(self) -> Option<Duration> {
        match self {
            Self::Retry { delay } => Some(delay),
            _ => None,
        }
    }

    pub fn dead_reason(self) -> Option<DeadReason> {
        match self {
            Self::Dead { reason } => Some(reason),
            _ => None,
        }
    }
}

/// Which dead letters to act on.
///
/// Every field is optional, but [`Store::replay_many`] takes a `limit` separately
/// and requires one. An unfiltered replay of a queue holding a million deliveries is
/// almost never what someone meant to do.
#[derive(Debug, Clone, Default)]
pub struct DeadLetterFilter {
    pub endpoint_id: Option<Uuid>,
    pub reason: Option<DeadReason>,
    pub event_type: Option<String>,
}
