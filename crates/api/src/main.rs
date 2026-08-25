use relay_api::{AppState, router, router_with_metrics};
use relay_store::Store;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    relay_metrics::logging::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://relay:relay@localhost:5433/relay".into());
    let bind = std::env::var("RELAY_API_BIND").unwrap_or_else(|_| "0.0.0.0:8080".into());

    let store = Store::connect(&database_url, 5).await?;
    store.migrate().await?;

    // Logged and carried on rather than fatal. An API that refuses to accept events
    // because it could not install a counter has turned a missing dashboard into a
    // lost webhook.
    let app = match relay_metrics::Exporter::install() {
        // Deliberately no queue gauges here. Those describe rows in a database this
        // process shares with the dispatcher, and the dispatcher is the one that
        // reports them — two reporters would double every panel that sums across
        // instances.
        Ok(exporter) => router_with_metrics(AppState { store }, exporter),
        Err(e) => {
            tracing::error!(error = %e, "metrics recorder could not be installed");
            router(AppState { store })
        }
    };

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, "relay-api listening");
    axum::serve(listener, app).await?;
    Ok(())
}
