//! The sender.
//!
//! Two layers. [`Sender`] does one delivery: sign it, send it, record what happened.
//! [`Pool`] runs many of those at once, claiming batches and bounding how many
//! requests are in flight.
//!
//! [`Reaper`] is the third piece and runs alongside them: workers die holding work,
//! and something has to notice.
//!
//! Still absent: retries, backoff, breakers, rate limits. Every failure here is
//! terminal, which is wrong and is M3's job to fix. Building the boring version
//! first is not laziness — once retries exist, a bug in signing looks exactly like
//! a bug in retrying, and telling them apart depends on the single-shot path
//! already being boringly reliable.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use relay_store::{DeliveryStatus, PendingDelivery, Store};
use tokio::{sync::Semaphore, task::JoinSet};
use uuid::Uuid;

/// Customer error pages can be enormous. Store enough to debug with, no more.
const SNIPPET_BYTES: usize = 2048;

/// Ceiling on one outbound request, connect included.
///
/// Public because the reaper's lease has to outlast it. If a lease could expire
/// while a request were still running, the reaper would return the row to the queue
/// and a second worker would send it while the first was still waiting on a reply.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Time to establish a connection, inside [`REQUEST_TIMEOUT`].
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error(transparent)]
    Store(#[from] relay_store::StoreError),
    #[error("http client: {0}")]
    Client(#[from] reqwest::Error),
}

/// What happened to one attempt.
///
/// M1 only distinguishes success from failure. M3 splits `Failed` into retryable
/// and permanent, which is the point at which this becomes a real classifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Succeeded { status: u16 },
    Failed { status: Option<u16>, error: String },
}

/// Cloning is cheap and shares state: `Store` clones share one connection pool and
/// `reqwest::Client` clones share one connection pool too. The worker pool hands a
/// clone to every spawned task, so this must not mean "open more sockets".
#[derive(Clone)]
pub struct Sender {
    store: Store,
    client: reqwest::Client,
    /// Identifies which process holds a claim. The reaper uses it to attribute
    /// stranded work.
    worker_id: String,
}

impl Sender {
    pub fn new(store: Store) -> Self {
        let client = reqwest::Client::builder()
            // Three separate limits. A missing *total* timeout is the classic way a
            // worker pool dies: a per-read timeout resets on every byte, so a slow
            // trickle can hold a connection open indefinitely.
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("reqwest client builds with static configuration");
        Self {
            store,
            client,
            worker_id: format!("sender-{}", uuid::Uuid::new_v4()),
        }
    }

    /// Take the next due delivery, if any, and attempt it.
    pub async fn deliver_next(&self) -> Result<Option<Outcome>, SendError> {
        let Some(pending) = self.store.next_pending_delivery().await? else {
            return Ok(None);
        };
        self.deliver(pending).await
    }

    /// Attempt one named delivery.
    pub async fn deliver_by_id(&self, delivery_id: Uuid) -> Result<Option<Outcome>, SendError> {
        let Some(pending) = self.store.pending_delivery_by_id(delivery_id).await? else {
            return Ok(None);
        };
        self.deliver(pending).await
    }

    async fn deliver(&self, p: PendingDelivery) -> Result<Option<Outcome>, SendError> {
        // Claim before sending, not after.
        //
        // Otherwise a failure to persist the result leaves the row `pending`, the
        // loop picks it up on the next pass, and the same webhook is sent again —
        // and again, for as long as the write keeps failing. The endpoint sees an
        // unbounded flood and nothing here reports a problem.
        if !self.store.claim(p.delivery_id, &self.worker_id).await? {
            return Ok(None);
        }
        self.deliver_claimed(p).await.map(Some)
    }

    /// Send a delivery that has *already* been claimed, and record the outcome.
    ///
    /// Separate from [`Sender::deliver`] because the worker pool claims a whole
    /// batch in one query and then fans the rows out. Calling this with an
    /// unclaimed row would reintroduce the duplicate-send bug, so the only callers
    /// are ones that have just claimed it.
    pub async fn deliver_claimed(&self, p: PendingDelivery) -> Result<Outcome, SendError> {
        let timestamp = unix_now();

        // Sign the stored bytes, and send those same bytes. Nothing in between may
        // parse and re-encode the payload: JSON key order is not defined, and the
        // signature covers bytes rather than meaning.
        let signature =
            relay_domain::signature::sign(p.secret.as_bytes(), timestamp, &p.raw_payload);

        let started = Instant::now();
        let result = self
            .client
            .post(&p.url)
            .header("content-type", "application/json")
            .header("relay-timestamp", timestamp.to_string())
            // A list, not a single value, so a secret can be rotated with an overlap
            // window instead of a cutover. M8 fills in the second entry.
            .header("relay-signature", format!("v1={signature}"))
            // Stable across every attempt of this delivery. If this changed per
            // attempt, receivers could not deduplicate retries.
            .header("relay-delivery-id", p.delivery_id.to_string())
            .header("relay-event-type", &p.event_type)
            .body(p.raw_payload.clone())
            .send()
            .await;

        let latency_ms = started.elapsed().as_millis() as i32;

        let (outcome, http_status, error, snippet) = match result {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                let snippet = truncate(&body, SNIPPET_BYTES);
                if status.is_success() {
                    (
                        Outcome::Succeeded {
                            status: status.as_u16(),
                        },
                        Some(status.as_u16() as i32),
                        None,
                        Some(snippet),
                    )
                } else {
                    (
                        Outcome::Failed {
                            status: Some(status.as_u16()),
                            error: format!("HTTP {status}"),
                        },
                        Some(status.as_u16() as i32),
                        Some(format!("HTTP {status}")),
                        Some(snippet),
                    )
                }
            }
            Err(e) => (
                Outcome::Failed {
                    status: None,
                    error: e.to_string(),
                },
                None,
                Some(e.to_string()),
                None,
            ),
        };

        let (outcome_class, final_status) = match &outcome {
            Outcome::Succeeded { .. } => ("success", DeliveryStatus::Succeeded),
            // No retry policy yet, so every failure is terminal. M3 changes this.
            Outcome::Failed { .. } => ("failed", DeliveryStatus::Dead),
        };

        // Attempt row and final status commit together: an attempt without a status
        // describes a delivery that looks unfinished but has already been sent, and
        // a status without an attempt claims an outcome with no evidence for it.
        self.store
            .finish_attempt(
                p.delivery_id,
                p.attempt,
                final_status,
                http_status,
                latency_ms,
                outcome_class,
                error.as_deref(),
                snippet.as_deref(),
            )
            .await?;

        Ok(outcome)
    }
}

/// How the pool is sized.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Ceiling on requests in flight at once. The real limit on throughput, since a
    /// worker spends essentially all of its time waiting on someone else's server.
    pub workers: usize,
    /// Rows per claim. Larger amortises the query over more deliveries; too large
    /// and rows sit claimed — and so invisible to every other worker — while the
    /// batch ahead of them drains.
    pub batch_size: usize,
    /// How long to wait before asking again once the queue comes back empty.
    pub idle_poll: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            workers: 32,
            batch_size: 32,
            idle_poll: Duration::from_millis(250),
        }
    }
}

/// Claims batches of due deliveries and sends them concurrently.
///
/// The shape is one claim loop feeding many senders, rather than N independent
/// loops each claiming for themselves. Both are correct — `SKIP LOCKED` makes
/// concurrent claims safe either way — but one loop issues one query per batch
/// instead of N queries per batch, and it can size each claim to the capacity that
/// is actually free.
pub struct Pool {
    sender: Sender,
    config: PoolConfig,
    /// Permits are the in-flight bound. A task holds one for the whole request and
    /// releases it on completion, so the claim loop can ask how much room is left
    /// before deciding how many rows to take.
    capacity: Arc<Semaphore>,
}

impl Pool {
    pub fn new(store: Store, config: PoolConfig) -> Self {
        Self {
            sender: Sender::new(store),
            capacity: Arc::new(Semaphore::new(config.workers)),
            config,
        }
    }

    /// Claim what there is room for and spawn a task per delivery.
    ///
    /// Returns the number claimed, or `None` when every worker is busy — the
    /// caller must then wait for capacity rather than spinning on the database.
    async fn claim_and_spawn(&self, tasks: &mut JoinSet<()>) -> Result<Option<usize>, SendError> {
        // Reap finished tasks so the set does not grow without bound over the life
        // of the process. Non-blocking: anything still running is left alone.
        while tasks.try_join_next().is_some() {}

        let free = self.capacity.available_permits();
        if free == 0 {
            return Ok(None);
        }

        // Never claim more than can start immediately. A claimed row is invisible to
        // every other worker until it is finished or its lease expires, so claiming
        // ahead of capacity strands work behind whatever is already running.
        let want = free.min(self.config.batch_size);
        let batch = self
            .sender
            .store
            .claim_batch(want as i64, &self.sender.worker_id)
            .await?;
        let claimed = batch.len();

        for pending in batch {
            // Cannot block: `want` permits were free a moment ago and this is the
            // only task that acquires them, so the count has only risen since.
            let permit = self
                .capacity
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore is never closed");
            let sender = self.sender.clone();
            tasks.spawn(async move {
                let _permit = permit;
                let id = pending.delivery_id;
                match sender.deliver_claimed(pending).await {
                    Ok(outcome) => tracing::info!(delivery_id = %id, ?outcome, "delivered"),
                    // The row stays `inflight`, which is exactly right: it must not
                    // be resent while the outcome is unknown. The reaper returns it
                    // once the lease expires.
                    Err(e) => tracing::error!(delivery_id = %id, error = %e, "delivery failed"),
                }
            });
        }

        Ok(Some(claimed))
    }

    /// Claim one batch and wait for all of it to finish.
    ///
    /// Deterministic, which makes it the right entry point for tests. [`Pool::run`]
    /// is what production uses: it never waits for a batch to drain before claiming
    /// the next, so one slow endpoint cannot hold up the rest.
    pub async fn run_once(&self) -> Result<usize, SendError> {
        let mut tasks = JoinSet::new();
        let claimed = self.claim_and_spawn(&mut tasks).await?.unwrap_or(0);
        while tasks.join_next().await.is_some() {}
        Ok(claimed)
    }

    /// Drain the queue forever.
    pub async fn run(&self) -> ! {
        let mut tasks = JoinSet::new();
        loop {
            match self.claim_and_spawn(&mut tasks).await {
                // Queue empty. Nothing to do but wait.
                Ok(Some(0)) => tokio::time::sleep(self.config.idle_poll).await,
                // Work started. Go straight back for more — deliveries already in
                // flight keep running while the next batch is claimed, which is what
                // stops a hanging endpoint from stalling healthy ones.
                Ok(Some(_)) => {}
                // Every worker busy. Park until one finishes instead of hammering
                // the database with claims that can have nowhere to go.
                Ok(None) => {
                    let _ = self.capacity.acquire().await;
                }
                Err(e) => {
                    tracing::error!(error = %e, "claim failed");
                    tokio::time::sleep(self.config.idle_poll).await;
                }
            }
        }
    }
}

/// How often the reaper looks, and how long a lease lasts.
#[derive(Debug, Clone)]
pub struct ReaperConfig {
    /// How long a worker may hold a delivery before it is presumed dead.
    pub lease_ttl: Duration,
    /// How often to look. Sets the worst-case delay before a stranded delivery is
    /// rescued, so the real recovery time is `lease_ttl + interval`.
    pub interval: Duration,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        Self {
            // Three times the request timeout. Generous on purpose: rescuing early
            // sends a webhook twice, rescuing late delays it. The second is much
            // cheaper to be wrong about.
            lease_ttl: REQUEST_TIMEOUT * 3,
            interval: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error(
    "lease TTL {lease_ttl:?} must be longer than the request timeout {REQUEST_TIMEOUT:?}: \
     a lease that expires mid-request lets a second worker send the same delivery"
)]
pub struct LeaseTooShort {
    pub lease_ttl: Duration,
}

/// Returns deliveries stranded by dead workers to the queue.
///
/// The pool marks a row `inflight` before sending and only writes a final status
/// once the outcome is known. That is the correct order — it is what stops a
/// delivery being sent twice — but it means a worker that dies in between leaves a
/// row nobody owns and the claim query cannot see. This loop is what unsticks them.
pub struct Reaper {
    store: Store,
    config: ReaperConfig,
    rescued: AtomicU64,
}

impl Reaper {
    /// Fails if the lease could expire while a request is still in flight, which
    /// would turn the reaper from a safety net into a source of duplicates.
    pub fn new(store: Store, config: ReaperConfig) -> Result<Self, LeaseTooShort> {
        if config.lease_ttl <= REQUEST_TIMEOUT {
            return Err(LeaseTooShort {
                lease_ttl: config.lease_ttl,
            });
        }
        Ok(Self {
            store,
            config,
            rescued: AtomicU64::new(0),
        })
    }

    /// Deliveries rescued since start. Should stay at zero; a rising count means
    /// workers are dying.
    pub fn rescued(&self) -> u64 {
        self.rescued.load(Ordering::Relaxed)
    }

    pub async fn reap_once(&self) -> Result<u64, SendError> {
        let n = self
            .store
            .reap_expired_leases(self.config.lease_ttl)
            .await?;
        if n > 0 {
            self.rescued.fetch_add(n, Ordering::Relaxed);
            // Warn, not info. Zero is the normal value, so any of this is a report
            // that something upstream died.
            tracing::warn!(
                rescued = n,
                lease_ttl = ?self.config.lease_ttl,
                "returned stranded deliveries to the queue"
            );
        }
        Ok(n)
    }

    pub async fn run(&self) -> ! {
        loop {
            if let Err(e) = self.reap_once().await {
                tracing::error!(error = %e, "reaper failed");
            }
            tokio::time::sleep(self.config.interval).await;
        }
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after 1970")
        .as_secs() as i64
}

/// Truncate on a character boundary so the result is always valid UTF-8.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}
