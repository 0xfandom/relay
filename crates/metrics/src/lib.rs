//! What Relay reports about itself: its metrics, and the shape of its logs.
//!
//! Every metric name and label used anywhere in Relay is defined in this file, and
//! nothing else calls the `metrics` macros directly. That is the whole reason the
//! crate exists: a counter name is a string, a mistyped string is a brand new
//! time series rather than an error, and a dashboard built on the old name simply
//! goes flat. Recording through typed functions makes the typo a compile failure.
//!
//! Two processes export, not one. The API server reports what it does — how long
//! an ingest took and how it ended — and the dispatcher reports the queue and the
//! send path. Prometheus scrapes both; they are separate targets with separate
//! `instance` labels, which is the ordinary shape for a service made of more than
//! one process.
//!
//! The queue gauges are deliberately exported by the dispatcher *only*. They
//! describe rows in a shared database, so every process that exported them would
//! report the same numbers under a different `instance` label, and any dashboard
//! that summed across instances would multiply the queue by the replica count.

use std::time::Duration;

pub mod logging;

use axum::{Router, extract::State, response::IntoResponse, routing::get};
use metrics_exporter_prometheus::{BuildError, Matcher, PrometheusBuilder, PrometheusHandle};
use relay_store::Store;

// ------------------------------------------------------------------- the names
//
// Prometheus convention: a `relay_` namespace, a base unit in the name (seconds,
// never milliseconds), and `_total` on counters. Names are stable API — a
// dashboard, an alert and a runbook all point at them — so renaming one later is a
// breaking change even though nothing fails to compile.

/// Attempts that reached the point of a decision, by what was decided.
///
/// The label vocabulary is deliberately the same four values the attempt log
/// stores in `outcome_class`, so a number on the dashboard and a row in the
/// database can be reconciled without a translation table.
const ATTEMPTS: &str = "relay_delivery_attempts_total";
/// Deliveries put back without being sent, by which gate held them.
///
/// Overlaps `ATTEMPTS{outcome="deferred"}` on purpose: that one answers "how much
/// work is being deferred", this one answers "by what" — and during an incident
/// the second question is the one being asked.
const DEFERRALS: &str = "relay_deliveries_deferred_total";
/// Deliveries that gave up, by reason.
const DEAD: &str = "relay_deliveries_dead_total";
/// Deliveries never sent because the destination was not a public address.
const REFUSED: &str = "relay_deliveries_refused_total";
/// How long an outbound request took, success or failure.
const DELIVERY_SECONDS: &str = "relay_delivery_duration_seconds";

/// Breakers that opened. A counter, so it survives the breaker closing again —
/// the gauge below cannot tell "never tripped" from "tripped and recovered".
const BREAKER_TRIPS: &str = "relay_breaker_trips_total";
/// Probes issued against a recovering endpoint.
const PROBES: &str = "relay_breaker_probes_total";
/// Probes that closed a breaker. The gap between this and [`PROBES`] is how much
/// work is being spent on endpoints that are not coming back.
const PROBES_RECOVERED: &str = "relay_breaker_probes_recovered_total";

/// Deliveries returned to the queue after a worker died holding them. Should stay
/// flat; any slope at all is a report that something upstream is crashing.
const RESCUED: &str = "relay_reaper_rescued_total";
/// Idempotency keys swept after their window expired.
const KEYS_PRUNED: &str = "relay_idempotency_keys_pruned_total";

/// Ingest requests, by how they ended.
const INGEST: &str = "relay_ingest_total";
/// How long `POST /v1/events` took, measured around the whole handler.
///
/// The number this exists to prove is a *negative*: ingest does no delivery work,
/// so this must stay flat while endpoints are timing out. If it ever tracks
/// delivery latency, the two paths have become coupled and the API has inherited
/// somebody else's outage.
const INGEST_SECONDS: &str = "relay_ingest_duration_seconds";

/// Open deliveries by status.
const QUEUE_DEPTH: &str = "relay_queue_depth";
const OUTBOX_PUBLISHED: &str = "relay_outbox_published_total";
const OUTBOX_REQUEUED: &str = "relay_outbox_requeued_total";
const OUTBOX_BACKLOG: &str = "relay_outbox_backlog";
const BROKER_LAG: &str = "relay_broker_lag";
const BROKER_CONSUMED: &str = "relay_broker_consumed_total";
const BROKER_RECLAIMED: &str = "relay_broker_reclaimed_total";
const BROKER_STALE: &str = "relay_broker_stale_messages_total";
/// Pending deliveries whose time has come. The gap against `QUEUE_DEPTH{status=
/// "pending"}` is work that is waiting *on purpose* — a backoff, a deferral —
/// rather than work nobody is getting to.
const QUEUE_DUE: &str = "relay_queue_due";
/// How far past its due time the oldest pending delivery is.
const QUEUE_OLDEST: &str = "relay_queue_oldest_pending_age_seconds";
/// Endpoints in each breaker state.
const BREAKERS: &str = "relay_endpoint_breakers";
/// On-disk size of each of Relay's tables, indexes included.
///
/// Per table rather than a total, because a total cannot say *which* one stopped
/// being pruned — and the answer is almost always the attempt log, which is the only
/// table that grows with traffic rather than with customers.
const TABLE_BYTES: &str = "relay_table_bytes";
/// Daily partitions of the attempt log that currently exist.
const PARTITIONS: &str = "relay_attempt_partitions";
/// Rows that landed in the attempt log's default partition.
///
/// Should be zero forever. Anything here arrived while its own day had no partition,
/// and the recovery is manual — so this is the one retention number worth alerting
/// on rather than graphing.
const DEFAULT_PARTITION_ROWS: &str = "relay_attempt_default_partition_rows";

// ---------------------------------------------------------------- the exporter

/// The buckets an outbound request is sorted into.
///
/// Chosen against [`relay_dispatcher::REQUEST_TIMEOUT`], not from a template: the
/// last bucket has to sit above the timeout or every request that timed out lands
/// in `+Inf` and the histogram cannot distinguish "slow" from "never answered".
/// Dense at the bottom because a healthy webhook is tens of milliseconds and the
/// interesting question there is whether it moved to hundreds.
const DELIVERY_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0,
];

/// Ingest is a transaction against a local database and nothing else, so it lives
/// two orders of magnitude below a delivery and needs its own scale. Sharing the
/// delivery buckets would put every single request in the first one.
const INGEST_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
];

/// Relay's metrics, and the `/metrics` endpoint that renders them.
///
/// Cloning is cheap and shares the underlying registry.
#[derive(Clone)]
pub struct Exporter {
    handle: PrometheusHandle,
    /// Set only in the process that owns the queue. See the note at the top of the
    /// file: gauges over shared rows must have exactly one reporter.
    store: Option<Store>,
}

impl Exporter {
    /// Install the global recorder and describe every metric.
    ///
    /// Fails if a recorder is already installed, which can only happen if this is
    /// called twice. Returning the error rather than panicking matters in tests,
    /// where several may share a process.
    pub fn install() -> Result<Self, BuildError> {
        let handle = PrometheusBuilder::new()
            .set_buckets_for_metric(
                Matcher::Full(DELIVERY_SECONDS.to_string()),
                DELIVERY_BUCKETS,
            )?
            .set_buckets_for_metric(Matcher::Full(INGEST_SECONDS.to_string()), INGEST_BUCKETS)?
            .install_recorder()?;
        describe();
        initialise();
        Ok(Self {
            handle,
            store: None,
        })
    }

    /// Also export the queue gauges, which are read from the database on scrape.
    ///
    /// Call this in the dispatcher and nowhere else.
    pub fn with_queue_gauges(mut self, store: Store) -> Self {
        self.store = Some(store);
        self
    }

    /// Render the current values in Prometheus text format.
    ///
    /// Gauges are refreshed here, on scrape, rather than by a background sampler.
    /// A sampler would mean the reported queue is up to one interval old, and the
    /// number people look at during an incident is the one that must not lag. The
    /// queries behind it are two indexed aggregates, which is cheaper than the
    /// scrape's own network round trip.
    ///
    /// A failed refresh leaves the previous gauge values in place and logs. The
    /// alternative — a scrape that errors — takes down every panel including the
    /// counters, which are in memory and were never in doubt.
    pub async fn render(&self) -> String {
        if let Some(store) = &self.store
            && let Err(e) = refresh_gauges(store).await
        {
            tracing::error!(error = %e, "could not refresh queue gauges");
        }
        // Drops histogram data that nothing has reported into for a while, so an
        // endpoint that stops being used stops occupying memory forever.
        self.handle.run_upkeep();
        self.handle.render()
    }

    /// A router serving `GET /metrics`, to merge into an existing server.
    pub fn router(self) -> Router {
        Router::new()
            .route("/metrics", get(scrape))
            .with_state(self)
    }

    /// Serve `/metrics` on a port of its own until cancelled.
    ///
    /// For the dispatcher, which has no HTTP server otherwise. A separate port
    /// rather than sharing the API's is not an accident of the process layout: the
    /// two report different things, and a scrape target that only sometimes reaches
    /// the process it is describing is worse than no target at all.
    ///
    /// Errors are logged and swallowed. A dispatcher that refuses to start because
    /// its metrics port is taken has turned an observability problem into an outage,
    /// which is exactly backwards.
    pub async fn serve(self, bind: &str, cancel: tokio_util::sync::CancellationToken) {
        let listener = match tokio::net::TcpListener::bind(bind).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(%bind, error = %e, "metrics endpoint could not bind");
                return;
            }
        };
        tracing::info!(%bind, "metrics endpoint listening");
        let served = axum::serve(listener, self.router())
            .with_graceful_shutdown(async move { cancel.cancelled().await })
            .await;
        if let Err(e) = served {
            tracing::error!(error = %e, "metrics endpoint stopped");
        }
    }
}

async fn scrape(State(exporter): State<Exporter>) -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        exporter.render().await,
    )
}

/// Read the database-backed gauges.
async fn refresh_gauges(store: &Store) -> Result<(), relay_store::StoreError> {
    let q = store.queue_stats().await?;
    metrics::gauge!(QUEUE_DEPTH, "status" => "pending").set(q.pending as f64);
    metrics::gauge!(QUEUE_DEPTH, "status" => "inflight").set(q.inflight as f64);
    metrics::gauge!(QUEUE_DEPTH, "status" => "dead").set(q.dead as f64);
    metrics::gauge!(QUEUE_DUE).set(q.pending_due as f64);
    // An empty queue has no oldest item. Reporting zero would be a lie in the
    // dangerous direction — it reads identically to "nothing is behind", which is
    // also what a healthy queue looks like, so the panel could never distinguish
    // them. `NaN` renders as `Nan` in the text format and Prometheus treats it as
    // a gap rather than a value, which is exactly what it is.
    metrics::gauge!(QUEUE_OLDEST).set(q.oldest_pending_age_secs.unwrap_or(f64::NAN));

    for t in store.table_sizes().await? {
        metrics::gauge!(TABLE_BYTES, "table" => t.table_name).set(t.bytes as f64);
    }
    metrics::gauge!(PARTITIONS).set(store.attempt_partitions().await? as f64);
    metrics::gauge!(DEFAULT_PARTITION_ROWS)
        .set(store.attempts_in_default_partition().await? as f64);

    // Separate from `relay_queue_depth{status="pending"}` on purpose. A large backlog
    // here means the *publisher* is behind; a large pending count with this at zero
    // means the consumers are. One number could not tell those apart, and they have
    // opposite fixes.
    metrics::gauge!(OUTBOX_BACKLOG).set(store.outbox_backlog().await? as f64);

    let b = store.breaker_stats().await?;
    metrics::gauge!(BREAKERS, "state" => "closed").set(b.closed as f64);
    metrics::gauge!(BREAKERS, "state" => "open").set(b.open as f64);
    metrics::gauge!(BREAKERS, "state" => "half_open").set(b.half_open as f64);
    Ok(())
}

/// Register help text and units for everything.
///
/// Worth doing for a reason beyond politeness: a metric that has been described
/// appears in `/metrics` with its `# HELP` and `# TYPE` lines even before anything
/// has reported into it. Without that, a counter at zero is indistinguishable from
/// a counter that does not exist, and the first thing anyone does with a new
/// dashboard is wonder which one they are looking at.
fn describe() {
    metrics::describe_counter!(ATTEMPTS, "Delivery attempts by what was decided afterwards");
    metrics::describe_counter!(
        DEFERRALS,
        "Deliveries put back unsent, by which gate held them"
    );
    metrics::describe_counter!(DEAD, "Deliveries that gave up, by reason");
    metrics::describe_counter!(
        REFUSED,
        "Deliveries refused because the destination was not public"
    );
    metrics::describe_histogram!(
        DELIVERY_SECONDS,
        metrics::Unit::Seconds,
        "Outbound request duration"
    );
    metrics::describe_counter!(BREAKER_TRIPS, "Times a breaker opened");
    metrics::describe_counter!(PROBES, "Probes issued against a recovering endpoint");
    metrics::describe_counter!(PROBES_RECOVERED, "Probes that closed a breaker");
    metrics::describe_counter!(
        RESCUED,
        "Deliveries returned to the queue after a worker died"
    );
    metrics::describe_counter!(KEYS_PRUNED, "Expired idempotency keys deleted");
    metrics::describe_counter!(INGEST, "Ingest requests by how they ended");
    metrics::describe_histogram!(
        INGEST_SECONDS,
        metrics::Unit::Seconds,
        "POST /v1/events duration"
    );
    metrics::describe_gauge!(QUEUE_DEPTH, "Open deliveries by status");
    metrics::describe_counter!(OUTBOX_PUBLISHED, "Deliveries announced to the broker");
    metrics::describe_counter!(
        OUTBOX_REQUEUED,
        "Announced deliveries the sweep put back; should stay at zero"
    );
    metrics::describe_gauge!(
        OUTBOX_BACKLOG,
        "Due deliveries not yet announced; the publisher's own backlog"
    );
    metrics::describe_gauge!(BROKER_LAG, "Messages in the broker, by state");
    metrics::describe_counter!(BROKER_CONSUMED, "Messages taken from the broker");
    metrics::describe_counter!(
        BROKER_RECLAIMED,
        "Messages taken over from a consumer that stopped reporting"
    );
    metrics::describe_counter!(
        BROKER_STALE,
        "Messages naming a delivery that was no longer claimable"
    );
    metrics::describe_gauge!(QUEUE_DUE, "Pending deliveries whose time has come");
    metrics::describe_gauge!(
        QUEUE_OLDEST,
        metrics::Unit::Seconds,
        "How far past due the oldest pending delivery is"
    );
    metrics::describe_gauge!(BREAKERS, "Endpoints in each breaker state");
    metrics::describe_gauge!(
        TABLE_BYTES,
        metrics::Unit::Bytes,
        "On-disk size of each table, indexes included"
    );
    metrics::describe_gauge!(PARTITIONS, "Daily partitions of the attempt log");
    metrics::describe_gauge!(
        DEFAULT_PARTITION_ROWS,
        "Attempts that landed in the default partition; should always be zero"
    );
}

/// Report every counter at zero, and every label value it can ever carry.
///
/// The exporter renders a metric only once something has reported into it, so
/// without this a counter that has genuinely never fired is simply absent — and an
/// absent series and a series at zero look identical on a graph while meaning
/// opposite things. "No deliveries have died" is the healthiest possible state and
/// must be visible as a flat line at zero, not as an empty panel that reads the
/// same as a broken exporter.
///
/// It is also why the label values are enums: a vocabulary that can be enumerated
/// here is a vocabulary that cannot grow a surprise member at three in the morning.
///
/// Gauges are deliberately left out. They are read from the database on scrape and
/// have a correct value from the first render, and pre-setting them to zero would
/// publish a queue depth of zero from a process that does not report queue depth at
/// all.
fn initialise() {
    for outcome in ["success", "deferred", "retryable", "permanent"] {
        metrics::counter!(ATTEMPTS, "outcome" => outcome).increment(0);
    }
    for reason in [
        Deferral::RateLimit,
        Deferral::ConcurrencyCap,
        Deferral::BreakerOpen,
        Deferral::ProbeInFlight,
    ] {
        metrics::counter!(DEFERRALS, "reason" => reason.as_str()).increment(0);
    }
    for m in [
        OUTBOX_PUBLISHED,
        OUTBOX_REQUEUED,
        BROKER_CONSUMED,
        BROKER_RECLAIMED,
        BROKER_STALE,
    ] {
        metrics::counter!(m).increment(0);
    }
    // The values `deliveries.dead_reason` is constrained to.
    for reason in ["permanent_failure", "attempts_exhausted", "refused"] {
        metrics::counter!(DEAD, "reason" => reason).increment(0);
    }
    for outcome in [
        Ingest::Accepted,
        Ingest::Replayed,
        Ingest::Rejected,
        Ingest::Error,
    ] {
        metrics::counter!(INGEST, "outcome" => outcome.as_str()).increment(0);
    }
    metrics::counter!(REFUSED).increment(0);
    metrics::counter!(BREAKER_TRIPS).increment(0);
    metrics::counter!(PROBES).increment(0);
    metrics::counter!(PROBES_RECOVERED).increment(0);
    metrics::counter!(RESCUED).increment(0);
    metrics::counter!(KEYS_PRUNED).increment(0);

    // Registered without an observation, so the buckets exist and read zero rather
    // than the whole histogram being missing.
    let _ = metrics::histogram!(DELIVERY_SECONDS);
    let _ = metrics::histogram!(INGEST_SECONDS);
}

// ------------------------------------------------------------- recording sites
//
// One function per thing that happens, taking already-typed values. Callers cannot
// invent a label, and adding a new label value is a change in this file rather than
// a surprise in the time series database three weeks later.

/// One attempt was recorded, and this is what was decided. `outcome` is the
/// attempt log's own vocabulary: `success`, `deferred`, `retryable` or `permanent`.
///
/// Separate from [`sent`] because not every recorded attempt involved a request. A
/// refused destination and a deferral both produce a row in the log and neither
/// produced a duration, and folding a zero into the latency histogram for them
/// would pull every percentile towards requests that never happened.
pub fn attempt(outcome: &'static str) {
    metrics::counter!(ATTEMPTS, "outcome" => outcome).increment(1);
}

/// A request actually went out and took this long, whatever came back.
///
/// Failures are timed too, and deliberately: a timeout is the slowest possible
/// delivery and excluding it would make an endpoint look fastest at the moment it
/// stopped working.
pub fn sent(took: Duration) {
    metrics::histogram!(DELIVERY_SECONDS).record(took.as_secs_f64());
}

/// A delivery was put back without being sent.
pub fn deferred(reason: Deferral) {
    attempt("deferred");
    metrics::counter!(DEFERRALS, "reason" => reason.as_str()).increment(1);
}

/// Which gate held a delivery back.
///
/// An enum rather than a string, so the set of reasons is closed. Label cardinality
/// is the failure mode that kills a metrics system, and it always arrives as
/// somebody passing a value that seemed fine at the call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deferral {
    RateLimit,
    ConcurrencyCap,
    BreakerOpen,
    ProbeInFlight,
}

impl Deferral {
    pub fn as_str(self) -> &'static str {
        match self {
            Deferral::RateLimit => "rate_limit",
            Deferral::ConcurrencyCap => "concurrency_cap",
            Deferral::BreakerOpen => "breaker_open",
            Deferral::ProbeInFlight => "probe_in_flight",
        }
    }
}

/// A delivery reached the dead letter queue. `reason` is the stored
/// `permanent_failure` or `attempts_exhausted`.
pub fn dead(reason: &'static str) {
    metrics::counter!(DEAD, "reason" => reason).increment(1);
}

/// A delivery was refused before it was sent.
pub fn refused() {
    metrics::counter!(REFUSED).increment(1);
}

pub fn breaker_tripped() {
    metrics::counter!(BREAKER_TRIPS).increment(1);
}

pub fn probe_issued() {
    metrics::counter!(PROBES).increment(1);
}

pub fn probe_recovered() {
    metrics::counter!(PROBES_RECOVERED).increment(1);
}

pub fn rescued(n: u64) {
    metrics::counter!(RESCUED).increment(n);
}

pub fn keys_pruned(n: u64) {
    metrics::counter!(KEYS_PRUNED).increment(n);
}

/// Deliveries announced to the broker.
pub fn published(n: u64) {
    metrics::counter!(OUTBOX_PUBLISHED).increment(n);
}

/// Deliveries the reconciliation sweep put back, because they were announced and
/// then nothing happened.
///
/// Should stay at zero. Anything else means messages are going missing between the
/// publisher and the consumers, and this is the only thing that would say so.
pub fn requeued(n: u64) {
    metrics::counter!(OUTBOX_REQUEUED).increment(n);
}

/// Messages taken from the broker.
pub fn consumed(n: u64) {
    metrics::counter!(BROKER_CONSUMED).increment(n);
}

/// Messages taken over from a consumer that stopped reporting.
pub fn reclaimed(n: u64) {
    metrics::counter!(BROKER_RECLAIMED).increment(n);
}

/// How much the broker is holding.
///
/// Set by whoever is talking to the broker rather than refreshed on scrape with the
/// database gauges, because the metrics crate deliberately knows nothing about the
/// broker — it is optional, and a Relay without one should not carry a Redis client.
///
/// Absent entirely when no broker is configured, which is the honest answer: zero
/// would claim an empty broker where there is none.
pub fn broker_lag(unread: u64, unacked: u64) {
    metrics::gauge!(BROKER_LAG, "state" => "unread").set(unread as f64);
    metrics::gauge!(BROKER_LAG, "state" => "unacked").set(unacked as f64);
}

/// Messages naming a delivery that was no longer claimable.
///
/// The ordinary cost of at-least-once delivery, not an error: a redelivered message,
/// or one whose row another consumer already took. Worth counting because a ratio
/// that climbs means the broker is redelivering far more than it should.
pub fn stale_message() {
    metrics::counter!(BROKER_STALE).increment(1);
}

/// How an ingest request ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ingest {
    /// A new event was created.
    Accepted,
    /// An idempotency key matched an earlier request; nothing new was created.
    Replayed,
    /// The caller sent something unusable. Their bug, and worth separating from
    /// ours — a spike here is a customer deploying a change, a spike in `Error` is
    /// us.
    Rejected,
    Error,
}

impl Ingest {
    pub fn as_str(self) -> &'static str {
        match self {
            Ingest::Accepted => "accepted",
            Ingest::Replayed => "replayed",
            Ingest::Rejected => "rejected",
            Ingest::Error => "error",
        }
    }
}

pub fn ingest(outcome: Ingest, took: Duration) {
    metrics::counter!(INGEST, "outcome" => outcome.as_str()).increment(1);
    metrics::histogram!(INGEST_SECONDS).record(took.as_secs_f64());
}
