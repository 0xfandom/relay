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
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};
use relay_domain::{
    backoff::Backoff,
    breaker::{self, State as BreakerState},
    outcome::{Class, Disposition, Transport, classify_status, classify_transport, disposition},
    rate_limit::{Rate, Take},
    transport,
    url_guard::{Policy, Refused},
};
use relay_store::{AttemptResult, DeadReason, DeliveryStatus, PendingDelivery, Store};
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use uuid::Uuid;

pub mod source;

pub use source::{Claimed, Polling, Receipt, Source};

/// Customer error pages can be enormous. Store enough to debug with, no more.
const SNIPPET_BYTES: usize = 2048;

/// Every bound on one outbound request.
///
/// Three timeouts rather than one, because they fail differently. A missing *total*
/// timeout is the classic way a worker pool dies: a per-read timeout resets on every
/// byte, so an endpoint dribbling one byte every fifty milliseconds satisfies it
/// forever while holding a worker until the process is restarted. The read timeout
/// catches the opposite case cheaply — a connection that goes silent mid-response is
/// abandoned in seconds rather than waiting out the whole budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestLimits {
    /// Time to establish a connection.
    pub connect: Duration,
    /// Longest gap between two reads before the response is abandoned.
    pub read: Duration,
    /// Ceiling on the whole request, connect included. The only one that bounds a
    /// slow trickle.
    pub total: Duration,
    /// Largest payload Relay will send.
    ///
    /// The ingest API already refuses anything bigger, so this normally cannot
    /// trigger. It exists for the case that is not normal: a cap lowered after rows
    /// were already stored under the old one.
    pub max_payload_bytes: usize,
    /// How much of a response body is read before the rest is discarded.
    pub max_response_bytes: usize,
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self {
            connect: CONNECT_TIMEOUT,
            read: READ_TIMEOUT,
            total: REQUEST_TIMEOUT,
            max_payload_bytes: MAX_PAYLOAD_BYTES,
            max_response_bytes: SNIPPET_BYTES,
        }
    }
}

/// How long the winner of a probe has to report back before another may be issued.
///
/// Longer than one request, so a probe that is merely slow is not raced by a second.
/// Finite, because a probe against an endpoint that accepts connections and never
/// answers would otherwise leave the breaker half-open forever with nobody allowed to
/// try again — a permanent outage produced by the thing meant to end one.
fn probe_deadline(total: Duration) -> Duration {
    total * 2
}

/// Longest a delivery waits after finding its endpoint at its concurrency cap.
/// Jittered down from here, so a saturated endpoint's backlog does not return in a
/// single wave.
const BUSY_DEFER: Duration = Duration::from_millis(250);

/// Ceiling on one outbound request, connect included.
///
/// Public because the reaper's lease has to outlast it. If a lease could expire
/// while a request were still running, the reaper would return the row to the queue
/// and a second worker would send it while the first was still waiting on a reply.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Time to establish a connection, inside [`REQUEST_TIMEOUT`].
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Longest silence between two reads of a response body.
///
/// Shorter than the total, and doing a different job: a connection that simply stops
/// talking is abandoned here in seconds instead of occupying a worker for the whole
/// budget. It cannot replace the total timeout — it resets on every byte received.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Largest payload Relay will send, matching the ingest cap.
pub const MAX_PAYLOAD_BYTES: usize = 256 * 1024;

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
    /// Held back by the endpoint's rate limit. Nothing was sent, so this is not a
    /// failure and does not spend an attempt.
    Deferred {
        after: Duration,
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

/// A DNS resolver that refuses to hand back internal addresses.
///
/// The last line of defence, and the only one without a gap. [`Sender`] also checks
/// before sending, but that check and the connection are two separate lookups, and a
/// resolver under the attacker's control can answer honestly for the first and
/// dishonestly for the second — DNS rebinding. Enforcing inside the client's own
/// resolver means the address that was approved *is* the address connected to.
struct GuardedResolver {
    policy: Policy,
}

impl reqwest::dns::Resolve for GuardedResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let policy = self.policy.clone();
        Box::pin(async move {
            let addrs = resolve_host(name.as_str(), 0).await;
            let ips: Vec<IpAddr> = addrs.iter().map(|a| a.ip()).collect();
            policy.check_addrs(&ips)?;
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// Look a host up, returning every address it answers with.
async fn resolve_host(host: &str, port: u16) -> Vec<SocketAddr> {
    // A bare address needs no lookup, and asking the resolver about one invites an
    // unnecessary round trip.
    if let Ok(ip) = host.parse::<IpAddr>() {
        return vec![SocketAddr::new(ip, port)];
    }
    // Brackets around an IPv6 literal are URL syntax, not part of the address.
    if let Some(inner) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']'))
        && let Ok(ip) = inner.parse::<IpAddr>()
    {
        return vec![SocketAddr::new(ip, port)];
    }
    tokio::net::lookup_host((host, port))
        .await
        .map(|it| it.collect())
        .unwrap_or_default()
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
    config: SenderConfig,
    /// Shared with every clone of this sender, so the whole worker pool draws on one
    /// bucket per endpoint rather than one each.
    limiter: Arc<Limiter>,
    /// Also shared. A per-clone bulkhead would bound nothing, since the pool makes
    /// one clone per delivery.
    bulkhead: Arc<Bulkhead>,
    /// Probes issued, and how they went. Shared with every clone.
    ///
    /// Worth counting separately from ordinary deliveries: a rising probe count with
    /// no recoveries is an endpoint that is never coming back, and the two numbers
    /// together are the only place the breaker's behaviour is visible from outside.
    probes: Arc<AtomicU64>,
    probes_recovered: Arc<AtomicU64>,
    /// Bytes of response body actually pulled off the wire, across every delivery.
    ///
    /// Counted so the cap is provable rather than asserted. An endpoint returning an
    /// eight-megabyte error page must not cost eight megabytes of reads, and the
    /// only way to show that is to count what was read.
    response_bytes_read: Arc<AtomicU64>,
}

/// How one delivery attempt behaves.
#[derive(Debug, Clone)]
pub struct SenderConfig {
    pub backoff: Backoff,
    /// Where deliveries are allowed to go. Strict by default — see [`Policy`].
    pub policy: Policy,
    /// Whether to enforce the endpoint's configured rate.
    ///
    /// On by default, because a limiter that has to be switched on protects nobody.
    /// Off only in tests that are exercising something else and would otherwise have
    /// to reason about token arithmetic to understand a failure.
    pub rate_limit: bool,
    /// How many requests may be in flight, in total and per endpoint.
    pub limits: Limits,
    /// The bounds on any one request: three timeouts and two size caps.
    pub request: RequestLimits,
    /// The transports this sender can deliver through.
    ///
    /// Carried in the config rather than looked up statically so a test can point
    /// the chat transports at a local stand-in — otherwise they could only be
    /// exercised against the real Telegram, which means once, by hand, never again.
    pub transports: transport::Registry,
    /// When to stop delivering to an endpoint entirely, and how long for.
    ///
    /// `None` disables the breaker. Off only for tests that are exercising something
    /// else — an endpoint that fails five times in a row would otherwise trip it and
    /// change what the test is measuring.
    pub breaker: Option<breaker::Policy>,
}

impl Default for SenderConfig {
    fn default() -> Self {
        Self {
            backoff: Backoff::default(),
            policy: Policy::default(),
            rate_limit: true,
            limits: Limits::default(),
            request: RequestLimits::default(),
            transports: transport::Registry::default(),
            breaker: Some(breaker::Policy::default()),
        }
    }
}

impl Sender {
    pub fn new(store: Store) -> Self {
        Self::with_config(store, SenderConfig::default())
    }

    pub fn with_backoff(store: Store, backoff: Backoff) -> Self {
        Self::with_config(
            store,
            SenderConfig {
                backoff,
                ..Default::default()
            },
        )
    }

    pub fn with_config(store: Store, config: SenderConfig) -> Self {
        let client = reqwest::Client::builder()
            // Three separate limits, doing three different jobs — see
            // [`RequestLimits`]. The total is the one that matters most: a per-read
            // timeout resets on every byte, so without it an endpoint dribbling one
            // byte at a time holds a worker until the process is restarted.
            .connect_timeout(config.request.connect)
            // Resets on every byte received, so it catches a connection that has
            // gone silent and nothing else.
            .read_timeout(config.request.read)
            // The only bound on a response that keeps arriving slowly forever.
            .timeout(config.request.total)
            // Not following redirects is a security control as much as a simplicity
            // one: a `302` is the easiest way for a URL that passed validation to end
            // up pointing at an internal address.
            .redirect(reqwest::redirect::Policy::none())
            .dns_resolver(Arc::new(GuardedResolver {
                policy: config.policy.clone(),
            }))
            .build()
            .expect("reqwest client builds with static configuration");
        Self {
            store,
            client,
            worker_id: format!("sender-{}", uuid::Uuid::new_v4()),
            limiter: Arc::new(Limiter::new()),
            bulkhead: Arc::new(Bulkhead::new(config.limits)),
            probes: Arc::new(AtomicU64::new(0)),
            probes_recovered: Arc::new(AtomicU64::new(0)),
            response_bytes_read: Arc::new(AtomicU64::new(0)),
            config,
        }
    }

    /// Probes issued since start.
    pub fn probes(&self) -> u64 {
        self.probes.load(Ordering::Relaxed)
    }

    /// Probes that closed a breaker. The gap between this and [`Sender::probes`] is
    /// how much work is being spent on endpoints that are not coming back.
    pub fn probes_recovered(&self) -> u64 {
        self.probes_recovered.load(Ordering::Relaxed)
    }

    /// Response-body bytes read from the network since start.
    ///
    /// Should stay close to `deliveries x max_response_bytes` no matter how large
    /// the bodies actually are.
    pub fn response_bytes_read(&self) -> u64 {
        self.response_bytes_read.load(Ordering::Relaxed)
    }

    /// The in-flight caps this sender is enforcing.
    pub fn bulkhead(&self) -> &Bulkhead {
        &self.bulkhead
    }

    /// The buckets this sender is enforcing. Exposed so tests and, later, metrics can
    /// see them.
    pub fn limiter(&self) -> &Limiter {
        &self.limiter
    }

    /// Refuse a URL that points anywhere but the public internet.
    ///
    /// Checked at send time rather than only at registration. A domain that was
    /// public last week can be repointed at an internal address today, and endpoints
    /// stored before this existed were never checked at all.
    async fn check_destination(&self, url: &str) -> Result<(), Refused> {
        let parsed = reqwest::Url::parse(url).map_err(|_| Refused::NoHost)?;
        self.config.policy.check_scheme(parsed.scheme())?;
        // An empty host as well as a missing one. Refusing it by name rather than
        // letting it fall through to "resolved to nothing" keeps the error honest.
        let host = parsed
            .host_str()
            .filter(|h| !h.is_empty())
            .ok_or(Refused::NoHost)?;
        let port = parsed.port_or_known_default().unwrap_or(80);
        // Checked against the port that will actually be connected to rather than
        // the one the URL writes down: `https://host/` and `https://host:443/` are
        // the same destination and only one of them says so.
        self.config.policy.check_port(port)?;

        let addrs = resolve_host(host, port).await;
        let ips: Vec<IpAddr> = addrs.iter().map(|a| a.ip()).collect();
        self.config.policy.check_addrs(&ips)
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
            return AttemptResult::Dead {
                reason: DeadReason::PermanentFailure,
            };
        }

        let attempt = attempt.max(0) as u32;
        if !self.config.backoff.attempts_remain(attempt) {
            // Distinguished from a permanent failure because the response is
            // different: this one might well work now that the endpoint is back, and
            // is worth replaying. A permanent failure needs someone to change
            // something first.
            return AttemptResult::Dead {
                reason: DeadReason::AttemptsExhausted,
            };
        }

        // The endpoint's own answer wins when it gave one — a rate limiter knows
        // when its window resets and we do not. Clamped, so a header of `86400`
        // cannot park a delivery for a day.
        let delay = match retry_after {
            Some(requested) => self.config.backoff.retry_after(requested),
            None => self
                .config
                .backoff
                .next_delay(attempt, rand::random::<f64>()),
        };
        AttemptResult::Retry { delay }
    }

    /// Put a delivery back because its endpoint has no tokens left.
    ///
    /// Deliberately not a failed attempt. Nothing was sent, the endpoint was never
    /// asked, and there is no evidence about whether it would have worked — so the
    /// attempt counter is untouched. Charging for a deferral would let a busy
    /// endpoint's deliveries die in the dead letter queue having never had a single
    /// request made to them.
    ///
    /// The delay is the bucket's own answer to "when will there be a token", so a
    /// deferred delivery comes back to a bucket that has one rather than bouncing.
    async fn defer(&self, p: PendingDelivery, after: Duration) -> Result<Outcome, SendError> {
        tracing::debug!(
            delivery_id = %p.delivery_id,
            endpoint_id = %p.endpoint_id,
            ?after,
            "deferred: endpoint rate limit"
        );

        self.store
            .defer_delivery(
                p.delivery_id,
                p.attempt,
                after,
                "endpoint rate limit",
                &self.worker_id,
            )
            .await?;

        relay_metrics::deferred(relay_metrics::Deferral::RateLimit);
        Ok(Outcome::Deferred { after })
    }

    /// Put a delivery back because its endpoint already has all the requests it is
    /// allowed to have in flight.
    ///
    /// Like a rate-limit deferral this spends no attempt — nothing was sent. The
    /// delay is short and jittered rather than derived from anything: there is no way
    /// to know when a slot will free up, and a fixed delay would bring every deferred
    /// delivery for a saturated endpoint back at the same instant, only to defer them
    /// all again.
    async fn defer_busy(&self, p: PendingDelivery) -> Result<Outcome, SendError> {
        let after = BUSY_DEFER.mul_f64(rand::random::<f64>().clamp(0.1, 1.0));
        tracing::debug!(
            delivery_id = %p.delivery_id,
            endpoint_id = %p.endpoint_id,
            ?after,
            "deferred: endpoint at its concurrency cap"
        );

        self.store
            .defer_delivery(
                p.delivery_id,
                p.attempt,
                after,
                "endpoint concurrency cap",
                &self.worker_id,
            )
            .await?;

        relay_metrics::deferred(relay_metrics::Deferral::ConcurrencyCap);
        Ok(Outcome::Deferred { after })
    }

    /// Put a delivery back because its endpoint's breaker is open.
    ///
    /// Spends no attempt, like the other two deferrals: nothing was sent. This one
    /// matters most of the three, because an open breaker means the endpoint is
    /// *already* failing — charging attempts for the time it is cut off would empty
    /// every pending delivery's retry budget during the outage, and they would all be
    /// dead by the time it came back.
    ///
    /// The delay runs to the cooldown's expiry plus a jittered margin. Without the
    /// jitter every delivery blocked during the outage would return in the same
    /// instant the cooldown ends, which is the flood the breaker exists to prevent,
    /// merely scheduled.
    async fn defer_open_breaker(
        &self,
        p: PendingDelivery,
        until: DateTime<Utc>,
    ) -> Result<Outcome, SendError> {
        let remaining = (until - Utc::now()).to_std().unwrap_or(Duration::ZERO);
        let after = remaining + BUSY_DEFER.mul_f64(rand::random::<f64>().clamp(0.1, 1.0));

        tracing::debug!(
            delivery_id = %p.delivery_id,
            endpoint_id = %p.endpoint_id,
            ?after,
            "deferred: endpoint breaker is open"
        );

        self.store
            .defer_delivery(
                p.delivery_id,
                p.attempt,
                after,
                "endpoint breaker open",
                &self.worker_id,
            )
            .await?;

        relay_metrics::deferred(relay_metrics::Deferral::BreakerOpen);
        Ok(Outcome::Deferred { after })
    }

    /// Put a delivery back because another worker is probing this endpoint.
    ///
    /// A short wait, and short on purpose: the probe settles the question within one
    /// request timeout, and whichever way it goes this delivery wants to know. If it
    /// succeeded the breaker is closed and this can go out; if it failed the breaker
    /// is open again with a longer cooldown and this will be deferred properly on
    /// the next look. Jittered so the losers do not all come back together.
    async fn defer_probe_in_flight(&self, p: PendingDelivery) -> Result<Outcome, SendError> {
        let after = BUSY_DEFER.mul_f64(rand::random::<f64>().clamp(0.1, 1.0));
        tracing::debug!(
            delivery_id = %p.delivery_id,
            endpoint_id = %p.endpoint_id,
            ?after,
            "deferred: another worker is probing this endpoint"
        );

        self.store
            .defer_delivery(
                p.delivery_id,
                p.attempt,
                after,
                "probe in flight",
                &self.worker_id,
            )
            .await?;

        relay_metrics::deferred(relay_metrics::Deferral::ProbeInFlight);
        Ok(Outcome::Deferred { after })
    }

    /// Record a delivery that was never sent because it is too large.
    ///
    /// Permanent rather than retryable: the payload is stored and will not shrink,
    /// so every retry would fail identically while the endpoint waits for something
    /// that is never coming.
    async fn oversized(&self, p: PendingDelivery, cap: usize) -> Result<Outcome, SendError> {
        let error = format!(
            "payload is {} bytes, over the {cap}-byte limit",
            p.raw_payload.len()
        );
        tracing::warn!(delivery_id = %p.delivery_id, bytes = p.raw_payload.len(), cap,
            "refusing to send an oversized payload");

        self.store
            .finish_attempt(
                p.delivery_id,
                p.attempt,
                AttemptResult::Dead {
                    reason: DeadReason::PermanentFailure,
                },
                None,
                0,
                Disposition::Permanent.as_str(),
                Some(&error),
                None,
                &self.worker_id,
            )
            .await?;

        relay_metrics::attempt(Disposition::Permanent.as_str());
        relay_metrics::dead(DeadReason::PermanentFailure.as_str());
        Ok(Outcome::Failed {
            class: Class::Permanent,
            status: None,
            error,
        })
    }

    /// Record a delivery whose request could not be built at all.
    ///
    /// Permanent, and separated from a refusal because it is a different problem: a
    /// refusal means the destination is somewhere Relay will not go, this means the
    /// endpoint's own configuration is malformed. `telegram://` with no chat id
    /// after it will never become well formed by waiting.
    async fn unbuildable(
        &self,
        p: PendingDelivery,
        error: transport::BuildError,
    ) -> Result<Outcome, SendError> {
        let error = error.to_string();
        tracing::warn!(
            delivery_id = %p.delivery_id,
            endpoint_id = %p.endpoint_id,
            transport = %p.transport,
            reason = %error,
            "cannot build a request for this endpoint"
        );

        self.store
            .finish_attempt(
                p.delivery_id,
                p.attempt,
                AttemptResult::Dead {
                    reason: DeadReason::PermanentFailure,
                },
                None,
                0,
                Disposition::Permanent.as_str(),
                Some(&error),
                None,
                &self.worker_id,
            )
            .await?;

        relay_metrics::attempt(Disposition::Permanent.as_str());
        relay_metrics::dead(DeadReason::PermanentFailure.as_str());
        Ok(Outcome::Failed {
            class: Class::Permanent,
            status: None,
            error,
        })
    }

    /// Record a delivery that was never sent because its destination was refused.
    async fn refuse(&self, p: PendingDelivery, refused: Refused) -> Result<Outcome, SendError> {
        let error = refused.to_string();
        tracing::warn!(
            delivery_id = %p.delivery_id,
            endpoint_id = %p.endpoint_id,
            reason = %error,
            "refused to deliver to a non-public address"
        );

        self.store
            .finish_attempt(
                p.delivery_id,
                p.attempt,
                AttemptResult::Dead {
                    reason: DeadReason::Refused,
                },
                None,
                0,
                Disposition::Permanent.as_str(),
                Some(&error),
                // Deliberately no snippet. There is no response — and if there ever
                // were one, handing it to the party who chose the address is the
                // second half of the vulnerability, not just the first.
                None,
                &self.worker_id,
            )
            .await?;

        // Counted as an attempt with a permanent outcome, matching what was just
        // written to the log, but *not* as a delivery duration: nothing was sent,
        // and a zero in the latency histogram would drag the median of every panel
        // that reads it towards a request that never happened.
        relay_metrics::attempt(Disposition::Permanent.as_str());
        relay_metrics::dead(DeadReason::Refused.as_str());
        relay_metrics::refused();
        Ok(Outcome::Failed {
            class: Class::Permanent,
            status: None,
            error,
        })
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

    #[tracing::instrument(
        name = "delivery",
        skip_all,
        fields(
            delivery_id = %p.delivery_id,
            endpoint_id = %p.endpoint_id,
            event_type = %p.event_type,
            attempt = p.attempt,
            outcome = tracing::field::Empty,
        )
    )]
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

    /// Decide whether this delivery may go out right now.
    ///
    /// Extracted from the send path so the whole decision is one span and one
    /// return value. Every gate here runs before even a DNS lookup: a delivery that
    /// is going straight back on the queue should cost as little as possible on the
    /// way there.
    async fn gate(&self, p: &PendingDelivery) -> Result<Gated, SendError> {
        // The bulkhead goes first, and the order is load-bearing. Taking a token and
        // *then* finding no slot would spend that token on a request that was never
        // made, and an endpoint at its concurrency cap would quietly receive less
        // than its configured rate. Reserving first costs nothing — the reservation
        // is released on every path out of here.
        //
        // Non-blocking on purpose. Waiting for this endpoint's slot would tie a
        // worker to an endpoint that has stopped answering, which is exactly the
        // coupling the cap exists to break.
        let Some(reserved) = self.bulkhead.try_reserve(p.endpoint_id) else {
            tracing::debug!(gate = "bulkhead", "held: endpoint at its concurrency cap");
            return Ok(Gated::Busy);
        };

        if self.config.rate_limit {
            let rate = Rate::new(p.rate_per_second, p.burst);
            if let Take::Wait { after, .. } = self.limiter.take(p.endpoint_id, rate) {
                // `reserved` is dropped here, returning the slot before the delivery
                // goes back on the queue.
                tracing::debug!(gate = "rate_limit", ?after, "held: no token yet");
                return Ok(Gated::RateLimited { after });
            }
        }

        // Last gate before the request. An open breaker means the endpoint has
        // already failed `threshold` times in a row and every delivery to it is
        // costing a worker a full request timeout to learn what the last thousand
        // established.
        if self.config.breaker.is_none() {
            return Ok(Gated::Go {
                reserved,
                probing: false,
            });
        }

        match breaker_gate(p, Utc::now()) {
            Gate::Send => Ok(Gated::Go {
                reserved,
                probing: false,
            }),
            Gate::Probe => {
                // The cooldown has expired, but that is true for every worker
                // holding a delivery to this endpoint. Exactly one may go — a
                // server that has just come back after an hour down, met by the
                // whole backlog at once, is very likely to fall over again, and
                // the breaker would reopen with a longer cooldown. The outage
                // would extend itself.
                //
                // The database picks the winner; the losers wait for the probe's
                // deadline like any other blocked delivery.
                if self
                    .store
                    .claim_probe(p.endpoint_id, probe_deadline(self.config.request.total))
                    .await?
                {
                    self.probes.fetch_add(1, Ordering::Relaxed);
                    relay_metrics::probe_issued();
                    tracing::info!(gate = "breaker", "probing a recovering endpoint");
                    Ok(Gated::Go {
                        reserved,
                        probing: true,
                    })
                } else {
                    // Briefly, not until the probe's deadline. The deadline
                    // governs when a *new* probe may be claimed if this one
                    // never reports; it is not how long everybody else should
                    // wait. A probe resolves within one request timeout, and if
                    // it succeeds the breaker is closed by then — deferring the
                    // backlog for the full deadline would leave a recovered
                    // endpoint idle while its deliveries sat waiting on a
                    // question that had already been answered.
                    tracing::debug!(gate = "breaker", "held: another worker is probing");
                    Ok(Gated::ProbeInFlight)
                }
            }
            Gate::ProbeInFlight => {
                tracing::debug!(gate = "breaker", "held: another worker is probing");
                Ok(Gated::ProbeInFlight)
            }
            Gate::Blocked { until } => {
                tracing::debug!(gate = "breaker", %until, "held: breaker is open");
                Ok(Gated::BreakerOpen { until })
            }
        }
    }

    /// Send a delivery that has *already* been claimed, and record the outcome.
    ///
    /// Separate from [`Sender::deliver`] because the worker pool claims a whole
    /// batch in one query and then fans the rows out. Calling this with an
    /// unclaimed row would reintroduce the duplicate-send bug, so the only callers
    /// are ones that have just claimed it.
    pub async fn deliver_claimed(&self, p: PendingDelivery) -> Result<Outcome, SendError> {
        let delivery = tracing::Span::current();

        let (reserved, probing) = match self
            .gate(&p)
            .instrument(tracing::debug_span!("gate"))
            .await?
        {
            Gated::Go { reserved, probing } => (reserved, probing),
            // Every one of these returns the reservation on the way out: `Gated`
            // carries it only on the path that goes on to use it.
            Gated::Busy => return self.defer_busy(p).await,
            Gated::RateLimited { after } => return self.defer(p, after).await,
            Gated::BreakerOpen { until } => return self.defer_open_breaker(p, until).await,
            Gated::ProbeInFlight => return self.defer_probe_in_flight(p).await,
        };

        // Refused before a connection is opened, for the same reason a bad address
        // is: the request is not one Relay is willing to make. Normally unreachable,
        // because ingest already refuses anything larger — it exists for the case
        // where that cap was lowered after these rows were stored.
        if p.raw_payload.len() > self.config.request.max_payload_bytes {
            return self
                .oversized(p, self.config.request.max_payload_bytes)
                .await;
        }

        // Held for the whole request and released on the way out, panic included:
        // both permits live in `_slot` and a dropped permit is returned to its
        // semaphore however the scope is left.
        let _slot = self.bulkhead.enter(reserved).await;

        let timestamp = unix_now();

        // Which transport, and what request it wants made. Everything after this
        // point is identical whatever the destination is — that is the whole test of
        // whether the abstraction sits in the right place.
        //
        // An unknown transport string reads as HTTP. The CHECK constraint makes it
        // unreachable, and delivering the payload verbatim is the safe direction to
        // be wrong in: the alternative is refusing to deliver anything to an endpoint
        // because of a typo in a column.
        let kind = transport::Kind::parse(&p.transport).unwrap_or(transport::Kind::Http);
        let outbound = match self.config.transports.build(
            kind,
            &transport::Context {
                address: &p.url,
                credential: p.secret.reveal(),
                previous_credential: p.previous_secret.as_ref().map(|s| s.reveal()),
                event_type: &p.event_type,
                delivery_id: &p.delivery_id.to_string(),
                payload: &p.raw_payload,
                timestamp,
            },
        ) {
            Ok(outbound) => outbound,
            // Permanent. Every way this fails describes the endpoint's own
            // configuration, and no amount of retrying makes a malformed chat id well
            // formed.
            Err(e) => return self.unbuildable(p, e).await,
        };

        // Checked against the URL that will actually be connected to, not the one
        // stored: for a chat transport those are different strings, and only the
        // built one can be resolved. A refused destination is permanent — no amount
        // of retrying makes an internal address public — and nothing about the
        // refusal reaches the response snippet, because the party who chose the
        // address must not learn what is listening at it.
        if let Err(refused) = self.check_destination(&outbound.url).await {
            return self.refuse(p, refused).await;
        }

        let started = Instant::now();
        // The one span that covers time spent on somebody else's server. Everything
        // else in a delivery is microseconds; if a delivery took ten seconds, it was
        // this.
        //
        // The *display* URL, never the real one: a Telegram bot token and a Discord
        // webhook token are both path segments, and this line would otherwise put one
        // in every log aggregator we own.
        let send = tracing::info_span!(
            "send",
            url = %outbound.display_url,
            transport = kind.as_str(),
            status = tracing::field::Empty
        );
        let mut request = self.client.post(&outbound.url);
        for (name, value) in &outbound.headers {
            request = request.header(*name, value);
        }
        let result = request
            .body(outbound.body)
            .send()
            .instrument(send.clone())
            .await;

        let took = started.elapsed();
        let latency_ms = took.as_millis() as i32;

        let (outcome, http_status, error, snippet, retry_after, transport) = match result {
            Ok(resp) => {
                let status = resp.status().as_u16();
                send.record("status", status);
                let class = classify_status(status);
                // Read before consuming the response body. An endpoint under a rate
                // limit knows exactly when its window resets, which is better
                // information than our schedule can derive.
                let retry_after = resp
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(relay_domain::backoff::parse_retry_after);
                let (snippet, read) =
                    read_snippet(resp, self.config.request.max_response_bytes).await;
                self.response_bytes_read
                    .fetch_add(read as u64, Ordering::Relaxed);
                if class == Class::Success {
                    (
                        Outcome::Succeeded { status },
                        Some(status as i32),
                        None,
                        Some(snippet),
                        retry_after,
                        None,
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
                        None,
                    )
                }
            }
            Err(e) => {
                // Kept as well as classified. The retry policy only needs to know
                // whether another try could work; the breaker needs to know whether
                // anything answered, and those are different questions — an
                // unparseable URL is permanent for one and no evidence at all for
                // the other.
                let transport = transport_of(&e);
                (
                    Outcome::Failed {
                        class: classify_transport(transport),
                        status: None,
                        error: e.to_string(),
                    },
                    None,
                    Some(e.to_string()),
                    None,
                    None,
                    Some(transport),
                )
            }
        };

        let (class, result) = match &outcome {
            Outcome::Succeeded { .. } => (Class::Success, AttemptResult::Succeeded),
            Outcome::Failed { class, .. } => {
                (*class, self.next_step(*class, p.attempt, retry_after))
            }
            // Unreachable: a deferral returns from `deliver_claimed` before any
            // request is made, so no outcome reaches here carrying one.
            Outcome::Deferred { .. } => unreachable!("a deferred delivery is never sent"),
        };

        // Whether the endpoint asked us to wait is a property of this attempt rather
        // than of the status code, so the classifier cannot know it on its own.
        let recorded = disposition(class, retry_after.is_some());

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
                recorded.as_str(),
                error.as_deref(),
                snippet.as_deref(),
                &self.worker_id,
            )
            .instrument(tracing::debug_span!("persist", outcome = recorded.as_str()))
            .await?;

        delivery.record("outcome", recorded.as_str());
        relay_metrics::attempt(recorded.as_str());
        relay_metrics::sent(took);
        if let Some(reason) = result.dead_reason() {
            relay_metrics::dead(reason.as_str());
        }

        // Fold this attempt into the endpoint's breaker. A separate write rather
        // than part of the transaction above: the delivery's own record is what must
        // never be lost, and coupling the breaker to it would mean a contended
        // endpoint row could fail an attempt that had already been made.
        //
        // The question asked is "did the endpoint answer", not "did this succeed" —
        // a 404 is a wrong path on a working server, and tripping on it would cut
        // off a healthy destination while hiding a problem that needs a person.
        if let Some(policy) = &self.config.breaker {
            let health = breaker::health(http_status.map(|s| s as u16), transport);
            match self
                .store
                .record_health(p.endpoint_id, health, policy)
                .await
            {
                Ok(r) => {
                    // Reported by the store rather than inferred from the state,
                    // because an attempt that was already in flight when the breaker
                    // opened leaves it open without having tripped it.
                    if r.tripped {
                        relay_metrics::breaker_tripped();
                    }
                    match r.state {
                        BreakerState::Open if probing => tracing::warn!(
                            endpoint_id = %p.endpoint_id,
                            "probe failed: breaker reopened with a longer cooldown"
                        ),
                        BreakerState::Open if r.tripped => tracing::warn!(
                            endpoint_id = %p.endpoint_id,
                            "breaker opened: deliveries to this endpoint are paused"
                        ),
                        // The gap between probes issued and probes that recovered is
                        // how much work is being spent on endpoints that are not
                        // coming back.
                        BreakerState::Closed if probing => {
                            self.probes_recovered.fetch_add(1, Ordering::Relaxed);
                            relay_metrics::probe_recovered();
                            tracing::info!(
                                endpoint_id = %p.endpoint_id,
                                "probe succeeded: endpoint recovered, deliveries resume"
                            );
                        }
                        _ => {}
                    }
                }
                Err(e) => tracing::error!(
                    endpoint_id = %p.endpoint_id, error = %e,
                    "could not record endpoint health"
                ),
            }
        }

        Ok(outcome)
    }
}

/// What the gates decided, and — on the one path that proceeds — the reservation
/// they took out.
///
/// The reservation travels in the `Go` variant rather than being taken by the
/// caller afterwards, so it is impossible to end up holding a slot on a path that
/// puts the delivery back. Every other variant drops it as it is constructed.
enum Gated {
    Go { reserved: Reserved, probing: bool },
    Busy,
    RateLimited { after: Duration },
    BreakerOpen { until: DateTime<Utc> },
    ProbeInFlight,
}

/// What the breaker says about one delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gate {
    /// The breaker is closed, or a probe is already under way.
    Send,
    /// The cooldown has expired and this delivery may try to become the probe.
    Probe,
    /// Another worker is already probing. Look again shortly.
    ProbeInFlight,
    /// The endpoint is cut off until `until`.
    Blocked { until: DateTime<Utc> },
}

/// Read the breaker state carried on the claim.
///
/// A pure function of the row, so it costs no query. The state can be a few
/// milliseconds stale — another worker may have tripped the breaker in between — and
/// that is the deliberate trade: a handful of extra requests to an endpoint that is
/// already failing, against one extra query per delivery forever.
fn breaker_gate(p: &PendingDelivery, now: DateTime<Utc>) -> Gate {
    match BreakerState::parse(&p.breaker_state) {
        // The overwhelming majority of deliveries take this branch.
        Some(BreakerState::Closed) | None => Gate::Send,
        Some(BreakerState::Open) => match p.breaker_probe_at {
            Some(at) if at <= now => Gate::Probe,
            Some(at) => Gate::Blocked { until: at },
            // An open breaker with no probe time would be cut off forever. The
            // schema forbids it; if it happens anyway, deliver rather than
            // blackhole the endpoint.
            None => Gate::Send,
        },
        // A probe is in flight. Letting others through would be the rush the
        // half-open state exists to prevent — but they should look again soon
        // rather than wait out the probe's whole deadline, because a probe that
        // succeeds closes the breaker and they can all go.
        Some(BreakerState::HalfOpen) => match p.breaker_probe_at {
            Some(at) if at > now => Gate::ProbeInFlight,
            // The deadline passed with no report. Whoever claimed it is gone, so
            // this delivery becomes the next probe rather than waiting forever.
            _ => Gate::Probe,
        },
    }
}

/// The token buckets, one per endpoint.
///
/// State lives in this process, which is the honest limitation to name up front:
/// two dispatcher replicas each keep their own bucket, so a rate of 10/s configured
/// on an endpoint is 10/s *per replica*. That is fine while Relay runs one
/// dispatcher, and the fix when it does not is a shared bucket rather than a
/// different algorithm — the arithmetic in [`relay_domain::rate_limit`] does not
/// change.
///
/// A plain `std::sync::Mutex` rather than an async one. The critical section is a
/// hash lookup and some floating-point arithmetic, with no `await` inside it, so an
/// async mutex would add a scheduling hop to save nothing.
#[derive(Debug, Default)]
pub struct Limiter {
    buckets: Mutex<HashMap<Uuid, Bucket>>,
}

#[derive(Debug)]
struct Bucket {
    tokens: f64,
    /// When `tokens` was last correct. Refill is computed from this rather than
    /// accrued on a timer, so an endpoint nobody is sending to costs nothing.
    at: Instant,
}

impl Limiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Ask for permission to send one delivery to `endpoint_id`.
    ///
    /// The rate is passed per call rather than stored, because it arrives on the
    /// claim: reconfiguring an endpoint then takes effect on its next delivery with
    /// nothing to invalidate.
    pub fn take(&self, endpoint_id: Uuid, rate: Rate) -> Take {
        let now = Instant::now();
        let mut buckets = self.buckets.lock().expect("bucket mutex is never poisoned");
        let bucket = buckets.entry(endpoint_id).or_insert_with(|| Bucket {
            // A new endpoint starts full. Starting empty would make the very first
            // delivery to every endpoint wait for no reason, and the burst allowance
            // exists precisely to absorb the first rush.
            tokens: rate.burst,
            at: now,
        });

        let take = rate.take(bucket.tokens, now.saturating_duration_since(bucket.at));
        bucket.tokens = take.tokens();
        bucket.at = now;
        take
    }

    /// How many endpoints are being tracked. Exposed for tests and for the metrics
    /// M7 will want.
    pub fn tracked(&self) -> usize {
        self.buckets.lock().expect("not poisoned").len()
    }
}

/// Caps on how many requests may be in flight at once.
///
/// Two limits, and they exist for different people. The per-endpoint cap is a
/// **bulkhead**: it protects other customers from one endpoint that has stopped
/// answering. The global cap protects Relay itself — sockets, file descriptors, and
/// the memory of every response being buffered at once — and has to be independent
/// of the worker count, because a worker spends most of its life waiting and the
/// two numbers are not the same question.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Total concurrent outbound requests from this process.
    pub max_in_flight: usize,
    /// Concurrent requests to any one endpoint.
    ///
    /// The number that decides whether a hanging endpoint is an incident or a
    /// footnote. With no cap, an endpoint that accepts connections and never replies
    /// absorbs every worker for a full request timeout, and every other customer's
    /// webhooks wait behind it.
    pub per_endpoint: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_in_flight: 64,
            // Deliberately small next to the pool. One endpoint may use an eighth of
            // it; the other seven eighths stay available to everybody else no matter
            // how badly that one behaves.
            per_endpoint: 8,
        }
    }
}

/// A reservation against one endpoint's share, not yet against the global pool.
///
/// Separate from [`Slot`] so the two are acquired in the right order and with the
/// right blocking behaviour — see [`Bulkhead::try_reserve`].
pub struct Reserved {
    _endpoint: tokio::sync::OwnedSemaphorePermit,
}

/// Permission to have one request in flight. Both permits are released when this is
/// dropped, including while unwinding from a panic.
pub struct Slot {
    _global: tokio::sync::OwnedSemaphorePermit,
    _endpoint: Reserved,
}

/// Enforces [`Limits`].
#[derive(Debug)]
pub struct Bulkhead {
    limits: Limits,
    global: Arc<Semaphore>,
    endpoints: Mutex<HashMap<Uuid, Arc<Semaphore>>>,
}

impl Bulkhead {
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            global: Arc::new(Semaphore::new(limits.max_in_flight.max(1))),
            endpoints: Mutex::new(HashMap::new()),
        }
    }

    /// Take one of this endpoint's slots, or fail immediately.
    ///
    /// Deliberately non-blocking, and that is the whole bulkhead. Waiting here would
    /// hold a worker task hostage to an endpoint that has stopped answering, which is
    /// precisely the coupling the per-endpoint cap exists to break. The caller defers
    /// the delivery instead and the worker is free within microseconds.
    pub fn try_reserve(&self, endpoint_id: Uuid) -> Option<Reserved> {
        let semaphore = {
            let mut endpoints = self.endpoints.lock().expect("not poisoned");
            endpoints
                .entry(endpoint_id)
                .or_insert_with(|| Arc::new(Semaphore::new(self.limits.per_endpoint.max(1))))
                .clone()
        };
        semaphore
            .try_acquire_owned()
            .ok()
            .map(|_endpoint| Reserved { _endpoint })
    }

    /// Wait for room in the global pool, holding the endpoint reservation.
    ///
    /// Blocking is right here and wrong above. The global pool is shared fairly and
    /// its holders are all actively sending, so a waiter is waiting on work that is
    /// definitely progressing — and no single endpoint can monopolise it, because the
    /// per-endpoint cap already bounds any one endpoint's share of it.
    pub async fn enter(&self, endpoint: Reserved) -> Slot {
        let global = self
            .global
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is never closed");
        Slot {
            _global: global,
            _endpoint: endpoint,
        }
    }

    /// Requests in flight right now.
    pub fn in_flight(&self) -> usize {
        self.limits
            .max_in_flight
            .max(1)
            .saturating_sub(self.global.available_permits())
    }

    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// How many endpoints have a semaphore. Exposed for tests and M7's metrics.
    pub fn tracked(&self) -> usize {
        self.endpoints.lock().expect("not poisoned").len()
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
    /// Where work is discovered. Polling Postgres by default; a broker when one is
    /// configured. The rest of this type cannot tell the difference, which is what
    /// makes the choice a deployment decision rather than a rewrite.
    source: Arc<dyn Source>,
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
        Self::with_config(
            store,
            config,
            SenderConfig {
                backoff,
                ..Default::default()
            },
        )
    }

    pub fn with_config(store: Store, config: PoolConfig, sender: SenderConfig) -> Self {
        let source = Arc::new(Polling::new(store.clone()));
        Self::with_source(store, config, sender, source)
    }

    /// As [`Pool::with_config`], reading work from somewhere other than the database.
    pub fn with_source(
        store: Store,
        config: PoolConfig,
        sender: SenderConfig,
        source: Arc<dyn Source>,
    ) -> Self {
        Self {
            sender: Sender::with_config(store, sender),
            source,
            capacity: Arc::new(Semaphore::new(config.workers)),
            config,
        }
    }

    /// Which source this pool is reading from, for logs.
    pub fn mode(&self) -> &'static str {
        self.source.mode()
    }

    /// Which process this pool's claims are stamped with.
    fn worker_id(&self) -> &str {
        &self.sender.worker_id
    }

    /// Claim what there is room for and spawn a task per delivery.
    ///
    /// Returns the number claimed, or `None` when every worker is busy — the
    /// caller must then wait for capacity rather than spinning on the database.
    // `debug` rather than the default `info`, because this span opens and closes on
    // every idle poll whether or not there was anything to claim. At the default poll
    // interval that is four log lines a second, forever, on a system doing nothing —
    // which drowns the lines that matter and costs real money to store. It still
    // exists at `debug`, so a delivery's parent batch is one `RUST_LOG` away.
    #[tracing::instrument(
        level = "debug",
        name = "batch",
        skip_all,
        fields(worker = %self.worker_id(), claimed = tracing::field::Empty)
    )]
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
        // Asked of the source rather than of the store, so the same loop runs whether
        // work is discovered by polling Postgres or pushed by a broker. Everything
        // after this line is identical in both modes, which is the property the
        // abstraction exists to guarantee.
        let batch = self
            .source
            .claim(want, &self.sender.worker_id)
            .instrument(tracing::debug_span!("claim", want))
            .await?;
        let claimed = batch.len();
        tracing::Span::current().record("claimed", claimed);

        for Claimed { pending, receipt } in batch {
            // Shutdown can land between claiming a batch and handing it out. Those
            // rows are `inflight` with nobody about to send them, so give them back
            // now rather than leaving the reaper to find them in half a minute.
            if cancel.is_cancelled() {
                if let Err(e) = self.sender.store.release(pending.delivery_id).await {
                    tracing::error!(delivery_id = %pending.delivery_id, error = %e,
                        "could not release a claimed delivery during shutdown");
                }
                // Settled even though nothing was sent. The row went back to the
                // queue, so the message has done its job; leaving it unacknowledged
                // would strand it in the broker until a reclaim, for a delivery that
                // is already pending again.
                if let Err(e) = self.source.settle(&receipt).await {
                    tracing::error!(delivery_id = %pending.delivery_id, error = %e,
                        "could not settle a released delivery during shutdown");
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
            let source = self.source.clone();
            // Built here, inside the batch span, so it is parented to the claim that
            // produced it — and then attached to the task explicitly.
            //
            // This is the part that does not happen on its own. A span is ambient to
            // the current thread, and `spawn` hands the future to whichever worker
            // thread is free, where the ambient span is something else entirely. A
            // task spawned without `.instrument()` is silently orphaned: its events
            // still appear, with no delivery id and no parent, and reconstructing
            // which of five hundred concurrent deliveries they belonged to is not
            // possible after the fact.
            let span = tracing::info_span!(
                "delivery",
                delivery_id = %pending.delivery_id,
                endpoint_id = %pending.endpoint_id,
                event_type = %pending.event_type,
                attempt = pending.attempt,
                outcome = tracing::field::Empty,
            );
            tasks.spawn(
                async move {
                    let _permit = permit;
                    match sender.deliver_claimed(pending).await {
                        Ok(outcome) => tracing::info!(?outcome, "delivered"),
                        // The row stays `inflight`, which is exactly right: it must
                        // not be resent while the outcome is unknown. The reaper
                        // returns it once the lease expires.
                        Err(e) => tracing::error!(error = %e, "delivery failed"),
                    }
                    // Settled on both paths. This says "this worker is finished with
                    // the message", not "the webhook arrived" — the delivery's own
                    // fate is already recorded in Postgres, and a message left
                    // unacknowledged because the send failed would simply be
                    // redelivered to attempt a row that is no longer due.
                    if let Err(e) = source.settle(&receipt).await {
                        tracing::error!(error = %e, "could not settle a finished delivery");
                    }
                }
                .instrument(span),
            );
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
    "lease TTL {lease_ttl:?} must be longer than the request timeout {request_timeout:?}: \
     a lease that expires mid-request lets a second worker send the same delivery"
)]
pub struct LeaseTooShort {
    pub lease_ttl: Duration,
    pub request_timeout: Duration,
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
        Self::with_request_timeout(store, config, REQUEST_TIMEOUT)
    }

    /// As [`Reaper::new`], checked against a sender whose total timeout is not the
    /// default.
    ///
    /// The check is the whole reason this parameter exists: the lease and the request
    /// timeout are configured independently, and a deployment that raises one without
    /// the other gets duplicate deliveries with nothing reporting why.
    pub fn with_request_timeout(
        store: Store,
        config: ReaperConfig,
        request_timeout: Duration,
    ) -> Result<Self, LeaseTooShort> {
        if config.lease_ttl <= request_timeout {
            return Err(LeaseTooShort {
                lease_ttl: config.lease_ttl,
                request_timeout,
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
            relay_metrics::rescued(n);
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

/// Tells the rest of the system that this process is still running.
///
/// Sits alongside the queue's own health signal rather than replacing it, and the
/// distinction is the point. Lateness in the queue says whether work is draining —
/// but it says nothing at all while the queue is empty, so a dispatcher that died
/// overnight is indistinguishable from one with nothing to do. This closes that gap
/// and nothing else: a beat proves the process is looping, never that the loop is
/// achieving anything.
///
/// Written to the database rather than exposed on a port because the reader is the
/// API, which already has a connection to the database and may be on another host.
/// A heartbeat endpoint would mean the API needs to know where every dispatcher
/// lives, which is a service discovery problem invented to avoid one `UPDATE`.
pub struct Heartbeat {
    store: Store,
    interval: Duration,
}

impl Heartbeat {
    /// Wants to be a small fraction of the API's staleness threshold, so a single
    /// slow write is not mistaken for a dead process.
    pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(5);

    pub fn new(store: Store, interval: Duration) -> Self {
        Self { store, interval }
    }

    pub async fn run(&self, cancel: CancellationToken) {
        loop {
            // Beat first, sleep second. On a cold start the API reports "the
            // dispatcher has never reported" until the first write lands, so
            // sleeping first would add a full interval of self-inflicted unreadiness
            // to every deploy.
            if let Err(e) = self
                .store
                .heartbeat(relay_store::HEARTBEAT_DISPATCHER)
                .await
            {
                // Logged and carried on. A failed beat is almost always the database
                // being briefly unavailable, and in that case the API cannot read the
                // heartbeat either — it is already reporting unready for a better
                // reason.
                tracing::warn!(error = %e, "heartbeat failed");
            }
            tokio::select! {
                _ = tokio::time::sleep(self.interval) => {}
                _ = cancel.cancelled() => {
                    tracing::info!("heartbeat stopped");
                    return;
                }
            }
        }
    }
}

/// How long each kind of history is kept, and how often the sweep runs.
///
/// Four windows rather than one, because the four are kept for different reasons and
/// a single number would have to satisfy the longest. The attempt log is a debugging
/// aid measured in weeks; a dead letter is money somebody is still owed and is kept
/// for a quarter.
#[derive(Debug, Clone)]
pub struct PrunerConfig {
    /// How long an idempotency key is honoured. A duplicate arriving after it
    /// creates a second event, so this is a product decision as much as a storage
    /// one.
    pub idempotency: Duration,
    /// How long the attempt log is kept. Enforced by dropping whole daily
    /// partitions, so this is rounded down to a day either way.
    pub attempts: Duration,
    /// How long a succeeded delivery's row is kept.
    ///
    /// Should not be shorter than `attempts`: deleting the delivery cascades into
    /// the attempt log, and cascading row-by-row into partitions is exactly the
    /// vacuum load the partitioning exists to avoid. Longer, and the partitions are
    /// gone before the cascade can find anything.
    pub succeeded: Duration,
    /// How long a dead delivery is kept. Longer than the rest: it is a webhook
    /// somebody is still owed, and the whole point of the dead letter queue is that
    /// it can be replayed once the underlying problem is fixed.
    pub dead: Duration,
    /// How many rows one delete touches before the sweep yields.
    ///
    /// Bounded so the sweep never takes a long lock on the table the claim query
    /// reads. The loop repeats until a pass deletes nothing.
    pub batch: i64,
    /// How far ahead daily partitions are created.
    ///
    /// Generous, and it costs one empty table per day. The window between "this
    /// partition is needed" and "this partition exists" is the only way rows reach
    /// the default partition, and pushing it out to weeks means this job can be
    /// broken for a fortnight without consequence.
    pub partition_days_ahead: i32,
    /// How often to sweep. Loose on purpose: history that outlives its window by an
    /// hour costs nothing, while sweeping every ten seconds costs a scan.
    pub interval: Duration,
}

impl Default for PrunerConfig {
    fn default() -> Self {
        Self {
            idempotency: relay_domain::idempotency::RETENTION,
            attempts: Duration::from_secs(30 * DAY),
            succeeded: Duration::from_secs(30 * DAY),
            dead: Duration::from_secs(90 * DAY),
            batch: 5_000,
            partition_days_ahead: 14,
            interval: Duration::from_secs(3600),
        }
    }
}

const DAY: u64 = 24 * 60 * 60;

/// What one sweep did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Swept {
    pub partitions_created: i64,
    pub partitions_dropped: Vec<String>,
    pub succeeded_deleted: u64,
    pub dead_deleted: u64,
    pub keys_pruned: u64,
}

/// Deletes idempotency keys that have outlived their window.
///
/// Lives beside the reaper rather than in the API for the same reason the reaper
/// does: the API is request-scoped and scales with traffic, so a sweep there would
/// run once per replica per request path and contend with the ingest it is meant to
/// protect. This process already exists to run periodic work.
///
/// Nothing depends on it for correctness — a key that is never pruned still
/// deduplicates. What it prevents is a table that grows as fast as the event table
/// and never shrinks, to answer a question nobody asks after the first hour.
pub struct Pruner {
    store: Store,
    config: PrunerConfig,
    pruned: AtomicU64,
}

impl Pruner {
    pub fn new(store: Store, config: PrunerConfig) -> Self {
        Self {
            store,
            config,
            pruned: AtomicU64::new(0),
        }
    }

    /// Idempotency keys deleted since start.
    pub fn pruned(&self) -> u64 {
        self.pruned.load(Ordering::Relaxed)
    }

    /// One full sweep: make partitions, drop partitions, delete rows, prune keys.
    ///
    /// The order is load-bearing. Partitions are created *first*, because a sweep
    /// that fails halfway must not leave tomorrow without a table to write into —
    /// that failure costs duplicate webhooks, and every other step here costs disk.
    /// Dropping comes before the row deletes so the cascade from a deleted delivery
    /// finds as few attempt rows as possible.
    pub async fn prune_once(&self) -> Result<Swept, SendError> {
        let partitions_created = self
            .store
            .ensure_attempt_partitions(self.config.partition_days_ahead)
            .await?;
        if partitions_created > 0 {
            tracing::info!(
                created = partitions_created,
                days_ahead = self.config.partition_days_ahead,
                "created attempt log partitions"
            );
        }

        let partitions_dropped = self
            .store
            .drop_attempt_partitions(as_days(self.config.attempts))
            .await?;
        if !partitions_dropped.is_empty() {
            // Info, not warn: this is the retention working. A partition drop frees
            // the whole day at once, with no vacuum and almost no WAL.
            tracing::info!(
                dropped = ?partitions_dropped,
                retention = ?self.config.attempts,
                "dropped expired attempt log partitions"
            );
        }

        let succeeded_deleted = self
            .delete_in_batches(DeliveryStatus::Succeeded, self.config.succeeded)
            .await?;
        let dead_deleted = self
            .delete_in_batches(DeliveryStatus::Dead, self.config.dead)
            .await?;

        let keys_pruned = self
            .store
            .prune_idempotency_keys(self.config.idempotency)
            .await?;
        if keys_pruned > 0 {
            self.pruned.fetch_add(keys_pruned, Ordering::Relaxed);
            relay_metrics::keys_pruned(keys_pruned);
            tracing::info!(
                pruned = keys_pruned,
                retention = ?self.config.idempotency,
                "deleted expired idempotency keys"
            );
        }

        // Should be zero forever. Anything here arrived while its own day had no
        // partition, and the recovery is manual — a new partition cannot be created
        // over a range the default already holds rows for.
        match self.store.attempts_in_default_partition().await {
            Ok(0) => {}
            Ok(n) => tracing::error!(
                rows = n,
                "attempts landed in the default partition; they must be moved by hand \
                 before that day's partition can be created"
            ),
            Err(e) => tracing::error!(error = %e, "could not read the default partition"),
        }

        Ok(Swept {
            partitions_created,
            partitions_dropped,
            succeeded_deleted,
            dead_deleted,
            keys_pruned,
        })
    }

    /// Delete terminal deliveries older than `older_than`, a batch at a time.
    ///
    /// Looping over bounded deletes rather than issuing one large one: a single
    /// statement covering a month of history holds a transaction and a pile of row
    /// locks on the table the claim query reads, and a delivery waiting behind a
    /// retention sweep is a webhook arriving late for no reason a customer could
    /// ever be told.
    async fn delete_in_batches(
        &self,
        status: DeliveryStatus,
        older_than: Duration,
    ) -> Result<u64, SendError> {
        let mut total = 0;
        // Bounded, so a sweep cannot run until the next one is due. Whatever is left
        // is taken by the following pass.
        for _ in 0..1_000 {
            let n = self
                .store
                .delete_deliveries(status, older_than, self.config.batch)
                .await?;
            total += n;
            if n < self.config.batch as u64 {
                break;
            }
        }
        if total > 0 {
            tracing::info!(
                deleted = total,
                status = status.as_str(),
                retention = ?older_than,
                "deleted expired deliveries"
            );
        }
        Ok(total)
    }

    pub async fn run(&self, cancel: CancellationToken) {
        loop {
            if let Err(e) = self.prune_once().await {
                tracing::error!(error = %e, "retention sweep failed");
            }
            tokio::select! {
                _ = tokio::time::sleep(self.config.interval) => {}
                _ = cancel.cancelled() => {
                    tracing::info!(pruned = self.pruned(), "pruner stopped");
                    return;
                }
            }
        }
    }
}

/// A retention window in whole days.
///
/// Rounded *up*, so a window is never shorter than configured. Partitions are daily,
/// so the granularity is a day whatever the number says, and erring towards keeping
/// data one day too long is the cheap direction to be wrong in.
fn as_days(d: Duration) -> i32 {
    d.as_secs().div_ceil(DAY).max(1) as i32
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after 1970")
        .as_secs() as i64
}

/// Read just enough of the response body to be useful in the log.
///
/// Streamed and stopped early rather than buffered and then truncated. A customer
/// error page can be enormous, and `text()` would pull the whole thing into memory
/// before anything trimmed it — with a pool of workers, a handful of endpoints
/// returning multi-megabyte HTML is enough to matter. Stopping at the cap means the
/// size of their error page is their problem, not ours.
async fn read_snippet(mut resp: reqwest::Response, cap: usize) -> (String, usize) {
    let mut buf: Vec<u8> = Vec::new();
    while buf.len() < cap {
        match resp.chunk().await {
            Ok(Some(chunk)) => buf.extend_from_slice(&chunk),
            // A body that fails midway is not worth failing the attempt over: the
            // status code has already told us what happened.
            Ok(None) | Err(_) => break,
        }
    }
    let read = buf.len();
    // Dropping the response here closes the connection without draining it, so the
    // remaining bytes are never pulled across. Reading to the end and then
    // truncating would produce the same string and cost the whole eight megabytes.
    drop(resp);
    (truncate(&String::from_utf8_lossy(&buf), cap), read)
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
