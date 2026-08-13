use std::{sync::Arc, time::Duration};

use relay_dispatcher::{Pool, PoolConfig, REQUEST_TIMEOUT, Reaper, ReaperConfig, SenderConfig};
use relay_domain::url_guard::Policy;
use relay_store::Store;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

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

    // Off unless explicitly enabled. Relay will make an HTTP request to any URL a
    // customer registers, from inside a private network, so allowing internal
    // addresses turns it into a server-side request forgery engine — the cloud
    // metadata service answers without authentication to anything on the box.
    // Local development needs it, because every receiver is on loopback there.
    let allow_private = std::env::var("RELAY_ALLOW_PRIVATE_ENDPOINTS")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    let sender_config = SenderConfig {
        backoff: Default::default(),
        policy: Policy { allow_private },
    };

    let store = Store::connect(&database_url, db_connections as u32).await?;
    store.migrate().await?;

    // Rejected at startup rather than tolerated, because a lease shorter than the
    // request timeout produces duplicate deliveries and nothing else would report it.
    let reaper = Arc::new(Reaper::new(store.clone(), reaper_config.clone())?);

    tracing::info!(
        workers = config.workers,
        batch_size = config.batch_size,
        db_connections,
        lease_ttl = ?reaper_config.lease_ttl,
        request_timeout = ?REQUEST_TIMEOUT,
        shutdown_deadline = ?config.shutdown_deadline,
        allow_private_endpoints = allow_private,
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

    let reaper_loop = {
        let cancel = cancel.clone();
        tokio::spawn(async move { reaper.run(cancel).await })
    };

    // Returns once cancelled and everything in flight has finished or hit the
    // deadline.
    Pool::with_config(store, config, sender_config)
        .run(cancel)
        .await;
    let _ = reaper_loop.await;

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
