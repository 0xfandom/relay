//! The sender.
//!
//! Two layers. [`Sender`] does one delivery: sign it, send it, record what happened.
//! [`Pool`] runs many of those at once, claiming batches and bounding how many
//! requests are in flight.
//!
//! [`Reaper`] is the third piece and runs alongside them: workers die holding work,
//! and something has to notice.
//!
//! A failed attempt is now reschedulable rather than fatal: the classifier decides
//! whether another try could plausibly work, and the backoff decides when.
//!
//! Still absent: breakers and rate limits, which are about refusing to send at all
//! rather than about when to send again.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use relay_domain::{
    backoff::Backoff,
    outcome::{Class, Transport, classify_status, classify_transport},
};
use relay_store::{AttemptResult, PendingDelivery, Store};
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Succeeded {
        status: u16,
    },
    Failed {
        /// Whether another attempt could plausibly succeed. Decided by
        /// [`relay_domain::outcome`], never inline here — one rule, one place.
        class: Class,
        status: Option<u16>,
        error: String,
    },
}

/// Translate this HTTP client's error into something the domain can classify.
///
/// The domain deliberately knows nothing about `reqwest`, so the mapping lives on
/// this side of the boundary. Everything unrecognised becomes `Other`, which is
/// retryable — the safe default, since giving up on a transient failure loses a
/// delivery while retrying a permanent one only wastes attempts.
fn transport_of(e: &reqwest::Error) -> Transport {
    if e.is_timeout() {
        Transport::Timeout
    } else if e.is_connect() {
        Transport::Connect
    } else if e.is_builder() {
        // A URL that will not parse. Not a network problem and not fixable by
        // waiting.
        Transport::Invalid
    } else {
        Transport::Other
    }
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
    backoff: Backoff,
}

impl Sender {
    pub fn new(store: Store) -> Self {
        Self::with_backoff(store, Backoff::default())
    }

    pub fn with_backoff(store: Store, backoff: Backoff) -> Self {
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
            backoff,
        }
    }

    /// What happens to a delivery after a failed attempt.
    ///
    /// The three-way split is the whole point of the classifier. A permanent failure
    /// stops immediately rather than spending eleven more attempts proving that a
    /// `404` is still a `404` — which is both waste and, from the endpoint's side,
    /// indistinguishable from being attacked.
    fn next_step(
        &self,
        class: Class,
        attempt: i32,
        retry_after: Option<Duration>,
    ) -> AttemptResult {
        if class != Class::Retryable {
            return AttemptResult::Dead;
        }

        let attempt = attempt.max(0) as u32;
        if !self.backoff.attempts_remain(attempt) {
            return AttemptResult::Dead;
        }

        // The endpoint's own answer wins when it gave one — a rate limiter knows
        // when its window resets and we do not. Clamped, so a header of `86400`
        // cannot park a delivery for a day.
        let delay = match retry_after {
            Some(requested) => self.backoff.retry_after(requested),
            None => self.backoff.next_delay(attempt, rand::random::<f64>()),
        };
        AttemptResult::Retry { delay }
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

        let (outcome, http_status, error, snippet, retry_after) = match result {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let class = classify_status(status);
                // Read before consuming the response body. An endpoint under a rate
                // limit knows exactly when its window resets, which is better
                // information than our schedule can derive.
                let retry_after = resp
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(relay_domain::backoff::parse_retry_after);
                let body = resp.text().await.unwrap_or_default();
                let snippet = truncate(&body, SNIPPET_BYTES);
                if class == Class::Success {
                    (
                        Outcome::Succeeded { status },
                        Some(status as i32),
                        None,
                        Some(snippet),
                        retry_after,
                    )
                } else {
                    (
                        Outcome::Failed {
                            class,
                            status: Some(status),
                            error: format!("HTTP {status}"),
                        },
                        Some(status as i32),
                        Some(format!("HTTP {status}")),
                        Some(snippet),
                        retry_after,
                    )
                }
            }
            Err(e) => (
                Outcome::Failed {
                    class: classify_transport(transport_of(&e)),
                    status: None,
                    error: e.to_string(),
                },
                None,
                Some(e.to_string()),
                None,
                None,
            ),
        };

        let (outcome_class, result) = match &outcome {
            Outcome::Succeeded { .. } => (Class::Success, AttemptResult::Succeeded),
            Outcome::Failed { class, .. } => {
                (*class, self.next_step(*class, p.attempt, retry_after))
            }
        };
        let outcome_class = outcome_class.as_str();

        // Attempt row and final status commit together: an attempt without a status
        // describes a delivery that looks unfinished but has already been sent, and
        // a status without an attempt claims an outcome with no evidence for it.
        self.store
            .finish_attempt(
                p.delivery_id,
                p.attempt,
                result,
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
    /// How long a shutdown will wait for in-flight requests before giving up on
    /// them. Must be finite: an endpoint that never answers would otherwise hold
    /// the process open until the orchestrator loses patience and sends SIGKILL,
    /// which is the ungraceful shutdown this exists to avoid.
    pub shutdown_deadline: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            workers: 32,
            batch_size: 32,
            idle_poll: Duration::from_millis(250),
            // Longer than one request timeout, so a request that started just before
            // the signal still gets its full budget to finish.
            shutdown_deadline: REQUEST_TIMEOUT + Duration::from_secs(5),
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
        Self::with_backoff(store, config, Backoff::default())
    }

    pub fn with_backoff(store: Store, config: PoolConfig, backoff: Backoff) -> Self {
        Self {
            sender: Sender::with_backoff(store, backoff),
            capacity: Arc::new(Semaphore::new(config.workers)),
            config,
        }
    }

    /// Claim what there is room for and spawn a task per delivery.
    ///
    /// Returns the number claimed, or `None` when every worker is busy — the
    /// caller must then wait for capacity rather than spinning on the database.
    async fn claim_and_spawn(
        &self,
        tasks: &mut JoinSet<()>,
        cancel: &CancellationToken,
    ) -> Result<Option<usize>, SendError> {
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
            // Shutdown can land between claiming a batch and handing it out. Those
            // rows are `inflight` with nobody about to send them, so give them back
            // now rather than leaving the reaper to find them in half a minute.
            if cancel.is_cancelled() {
                if let Err(e) = self.sender.store.release(pending.delivery_id).await {
                    tracing::error!(delivery_id = %pending.delivery_id, error = %e,
                        "could not release a claimed delivery during shutdown");
                }
                continue;
            }

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
        let claimed = self
            .claim_and_spawn(&mut tasks, &CancellationToken::new())
            .await?
            .unwrap_or(0);
        while tasks.join_next().await.is_some() {}
        Ok(claimed)
    }

    /// Drain the queue until cancelled, then finish what is in hand.
    pub async fn run(&self, cancel: CancellationToken) {
        let mut tasks = JoinSet::new();

        while !cancel.is_cancelled() {
            match self.claim_and_spawn(&mut tasks, &cancel).await {
                // Queue empty. Nothing to do but wait — and waking early on
                // cancellation is what keeps shutdown from taking a whole poll
                // interval to notice.
                Ok(Some(0)) => {
                    tokio::select! {
                        _ = tokio::time::sleep(self.config.idle_poll) => {}
                        _ = cancel.cancelled() => {}
                    }
                }
                // Work started. Go straight back for more — deliveries already in
                // flight keep running while the next batch is claimed, which is what
                // stops a hanging endpoint from stalling healthy ones.
                Ok(Some(_)) => {}
                // Every worker busy. Park until one finishes instead of hammering
                // the database with claims that can have nowhere to go.
                Ok(None) => {
                    tokio::select! {
                        _ = self.capacity.acquire() => {}
                        _ = cancel.cancelled() => {}
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "claim failed");
                    tokio::select! {
                        _ = tokio::time::sleep(self.config.idle_poll) => {}
                        _ = cancel.cancelled() => {}
                    }
                }
            }
        }

        self.drain(tasks).await;
    }

    /// Let in-flight deliveries finish, up to the deadline.
    ///
    /// Deliberately not cancelling them. A request that has already gone out may
    /// well have arrived, so dropping it mid-flight does not undo anything — it only
    /// throws away the answer, leaving a row that has to be reaped and re-sent to an
    /// endpoint that already has it. Waiting is both kinder and cheaper.
    async fn drain(&self, mut tasks: JoinSet<()>) {
        if tasks.is_empty() {
            tracing::info!("shutdown: nothing in flight");
            return;
        }

        tracing::info!(
            in_flight = tasks.len(),
            "shutdown: draining in-flight deliveries"
        );
        let drained = tokio::time::timeout(self.config.shutdown_deadline, async {
            while tasks.join_next().await.is_some() {}
        })
        .await;

        if drained.is_err() {
            // Whatever is left is stuck on an endpoint that is not answering. Their
            // rows stay `inflight` and the reaper returns them once the lease
            // expires, which is exactly the case it exists for.
            tracing::warn!(
                abandoned = tasks.len(),
                deadline = ?self.config.shutdown_deadline,
                "shutdown deadline exceeded; abandoned deliveries will be reaped"
            );
            tasks.abort_all();
        } else {
            tracing::info!("shutdown: all in-flight deliveries finished");
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

    pub async fn run(&self, cancel: CancellationToken) {
        loop {
            if let Err(e) = self.reap_once().await {
                tracing::error!(error = %e, "reaper failed");
            }
            tokio::select! {
                _ = tokio::time::sleep(self.config.interval) => {}
                _ = cancel.cancelled() => {
                    tracing::info!(rescued = self.rescued(), "reaper stopped");
                    return;
                }
            }
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
