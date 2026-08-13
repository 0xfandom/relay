use std::time::Duration;

use relay_dispatcher::{Pool, PoolConfig};
use relay_store::Store;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://relay:relay@localhost:5433/relay".into());

    let config = PoolConfig {
        workers: env_usize("RELAY_WORKERS", 32)?,
        batch_size: env_usize("RELAY_BATCH_SIZE", 32)?,
        idle_poll: Duration::from_millis(env_usize("RELAY_IDLE_POLL_MS", 250)? as u64),
    };

    // Connections are held only for the claim and for recording the outcome, never
    // across the outbound request, so the pool does not need one per worker. It does
    // need enough that workers finishing together are not queueing for one.
    let db_connections = env_usize("RELAY_DB_CONNECTIONS", 8)?;

    let store = Store::connect(&database_url, db_connections as u32).await?;
    store.migrate().await?;

    tracing::info!(
        workers = config.workers,
        batch_size = config.batch_size,
        db_connections,
        "relay-dispatcher started"
    );

    Pool::new(store, config).run().await
}

fn env_usize(key: &str, default: usize) -> anyhow::Result<usize> {
    match std::env::var(key) {
        Ok(v) => v
            .parse()
            .map_err(|_| anyhow::anyhow!("{key} must be a positive integer, got {v:?}")),
        Err(_) => Ok(default),
    }
}
