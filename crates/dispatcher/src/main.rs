use std::{sync::Arc, time::Duration};

use relay_dispatcher::{
    Limits, Pool, PoolConfig, Pruner, PrunerConfig, Reaper, ReaperConfig, RequestLimits,
    SenderConfig,
};
use relay_domain::url_guard::Policy;
use relay_store::Store;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    relay_metrics::logging::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://relay:relay@localhost:5433/relay".into());

    let config = PoolConfig {
        workers: env_usize("RELAY_WORKERS", 32)?,
        batch_size: env_usize("RELAY_BATCH_SIZE", 32)?,
        idle_poll: Duration::from_millis(env_usize("RELAY_IDLE_POLL_MS", 250)? as u64),
        shutdown_deadline: Duration::from_secs(
            env_usize("RELAY_SHUTDOWN_DEADLINE_SECS", 15)? as u64
        ),
    };

    // Connections are held only for the claim and for recording the outcome, never
    // across the outbound request, so the pool does not need one per worker. It does
    // need enough that workers finishing together are not queueing for one.
    let db_connections = env_usize("RELAY_DB_CONNECTIONS", 8)?;

    let reaper_config = ReaperConfig {
        lease_ttl: Duration::from_secs(env_usize("RELAY_LEASE_TTL_SECS", 30)? as u64),
        interval: Duration::from_secs(env_usize("RELAY_REAP_INTERVAL_SECS", 10)? as u64),
    };

    // How long a producer's retry is still recognised as a retry. Shortening it
    // trades storage for a wider window in which a duplicate creates a second event.
    let pruner_config = PrunerConfig {
        retention: Duration::from_secs(env_usize(
            "RELAY_IDEMPOTENCY_RETENTION_SECS",
            relay_domain::idempotency::RETENTION.as_secs() as usize,
        )? as u64),
        interval: Duration::from_secs(env_usize("RELAY_PRUNE_INTERVAL_SECS", 3600)? as u64),
    };

    // Where deliveries are allowed to go: which addresses, which scheme, which
    // ports. Strict unless explicitly relaxed, and read from one place shared with
    // the API — a divergence between what registration accepts and what the send
    // path allows is the whole failure mode this guards against.
    //
    // Relay will make an HTTP request to any URL a customer registers, from inside a
    // private network, so a permissive policy turns it into a server-side request
    // forgery engine — the cloud metadata service answers without authentication to
    // anything on the box.
    let policy = Policy::from_env();
    let allow_private = policy.allow_private;
    // Off only for a deliberate load test. A limiter that has to be switched on
    // protects nobody, and the thing it protects is somebody else's server.
    let rate_limit = std::env::var("RELAY_RATE_LIMIT")
        .map(|v| !(v == "false" || v == "0"))
        .unwrap_or(true);

    // Two different protections. The per-endpoint cap is a bulkhead — it stops one
    // customer's dead server from absorbing the pool. The global cap protects Relay's
    // own sockets and memory, and is independent of the worker count because a worker
    // spends most of its life waiting.
    let limits = Limits {
        max_in_flight: env_usize("RELAY_MAX_IN_FLIGHT", 64)?,
        per_endpoint: env_usize("RELAY_MAX_PER_ENDPOINT", 8)?,
    };

    // When to stop delivering to an endpoint entirely. Off only for a deliberate
    // load test — an endpoint that has failed five times in a row is costing a
    // worker a full request timeout per delivery to learn nothing new.
    let breaker = if std::env::var("RELAY_BREAKER")
        .map(|v| v == "false" || v == "0")
        .unwrap_or(false)
    {
        None
    } else {
        Some(relay_domain::breaker::Policy {
            threshold: env_usize("RELAY_BREAKER_THRESHOLD", 5)? as u32,
            cooldown: Duration::from_secs(env_usize("RELAY_BREAKER_COOLDOWN_SECS", 30)? as u64),
            max_cooldown: Duration::from_secs(
                env_usize("RELAY_BREAKER_MAX_COOLDOWN_SECS", 300)? as u64
            ),
        })
    };

    // Every bound on a single outbound request. The total timeout is the one that
    // has to exist: a per-read timeout resets on every byte, so without it an
    // endpoint dribbling one byte at a time holds a worker until this process is
    // restarted.
    let request = RequestLimits {
        connect: Duration::from_secs(env_usize("RELAY_CONNECT_TIMEOUT_SECS", 5)? as u64),
        read: Duration::from_secs(env_usize("RELAY_READ_TIMEOUT_SECS", 5)? as u64),
        total: Duration::from_secs(env_usize("RELAY_REQUEST_TIMEOUT_SECS", 10)? as u64),
        max_payload_bytes: env_usize(
            "RELAY_MAX_PAYLOAD_BYTES",
            relay_dispatcher::MAX_PAYLOAD_BYTES,
        )?,
        max_response_bytes: env_usize("RELAY_MAX_RESPONSE_BYTES", 2048)?,
    };

    let sender_config = SenderConfig {
        backoff: Default::default(),
        policy: policy.clone(),
        rate_limit,
        limits,
        request,
        breaker,
    };

    let store = Store::connect(&database_url, db_connections as u32).await?;
    store.migrate().await?;

    // Installed before anything can record, and only ever once per process: the
    // recorder is global, and every `relay_metrics` call made before this point is
    // silently dropped. Failing to install is logged rather than fatal — a
    // dispatcher that will not deliver webhooks because it could not set up a
    // counter is a worse outage than the one it was meant to help diagnose.
    let metrics_bind =
        std::env::var("RELAY_METRICS_BIND").unwrap_or_else(|_| "0.0.0.0:9091".into());
    let exporter = match relay_metrics::Exporter::install() {
        // The dispatcher owns the queue, so it is the one process that exports the
        // queue gauges. See the note in `relay-metrics`: two reporters of the same
        // database rows would double every panel that sums across instances.
        Ok(e) => Some(e.with_queue_gauges(store.clone())),
        Err(e) => {
            tracing::error!(error = %e, "metrics recorder could not be installed");
            None
        }
    };

    // Rejected at startup rather than tolerated, because a lease shorter than the
    // request timeout produces duplicate deliveries and nothing else would report it.
    let reaper = Arc::new(Reaper::with_request_timeout(
        store.clone(),
        reaper_config.clone(),
        request.total,
    )?);
    let pruner = Arc::new(Pruner::new(store.clone(), pruner_config.clone()));

    tracing::info!(
        workers = config.workers,
        batch_size = config.batch_size,
        db_connections,
        lease_ttl = ?reaper_config.lease_ttl,
        idempotency_retention = ?pruner_config.retention,
        request = ?request,
        shutdown_deadline = ?config.shutdown_deadline,
        policy = ?policy,
        rate_limit,
        max_in_flight = limits.max_in_flight,
        max_per_endpoint = limits.per_endpoint,
        breaker = ?breaker,
        %metrics_bind,
        "relay-dispatcher started"
    );

    if allow_private {
        tracing::warn!(
            "RELAY_ALLOW_PRIVATE_ENDPOINTS is on: deliveries may reach internal \
             addresses, including the cloud metadata service. Development only."
        );
    }

    // One token, every loop. A shutdown that stopped some loops and not others would
    // leave the reaper rescuing work that the pool is no longer around to send.
    let cancel = CancellationToken::new();

    let signalled = cancel.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("shutdown signal received");
        signalled.cancel();
    });

    let metrics_loop = exporter.map(|exporter| {
        let cancel = cancel.clone();
        let bind = metrics_bind.clone();
        tokio::spawn(async move { exporter.serve(&bind, cancel).await })
    });

    let reaper_loop = {
        let cancel = cancel.clone();
        tokio::spawn(async move { reaper.run(cancel).await })
    };

    let pruner_loop = {
        let cancel = cancel.clone();
        tokio::spawn(async move { pruner.run(cancel).await })
    };

    // Returns once cancelled and everything in flight has finished or hit the
    // deadline.
    Pool::with_config(store, config, sender_config)
        .run(cancel)
        .await;
    let _ = reaper_loop.await;
    let _ = pruner_loop.await;
    if let Some(metrics_loop) = metrics_loop {
        let _ = metrics_loop.await;
    }

    tracing::info!("relay-dispatcher stopped");
    Ok(())
}

/// Resolves on SIGTERM or Ctrl-C.
///
/// SIGTERM is the one that matters: it is what container runtimes send before
/// SIGKILL, so handling it is the difference between draining in-flight deliveries
/// and having them cut off on every deploy.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

fn env_usize(key: &str, default: usize) -> anyhow::Result<usize> {
    match std::env::var(key) {
        Ok(v) => v
            .parse()
            .map_err(|_| anyhow::anyhow!("{key} must be a positive integer, got {v:?}")),
        Err(_) => Ok(default),
    }
}
