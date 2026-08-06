use std::time::Duration;

use relay_dispatcher::Sender;
use relay_store::Store;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://relay:relay@localhost:5433/relay".into());

    let store = Store::connect(&database_url, 5).await?;
    store.migrate().await?;
    let sender = Sender::new(store);

    tracing::info!("relay-dispatcher started");

    // M1: poll one delivery at a time. M2 replaces this with a claim batch and a
    // pool of workers.
    loop {
        match sender.deliver_next().await {
            Ok(Some(outcome)) => tracing::info!(?outcome, "delivered"),
            Ok(None) => tokio::time::sleep(Duration::from_millis(250)).await,
            Err(e) => {
                tracing::error!(error = %e, "delivery loop error");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
