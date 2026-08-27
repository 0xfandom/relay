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

    // The same policy the dispatcher builds, from the same variables. Registration
    // is a courtesy check — the authority is the send path, which resolves the
    // address at the moment it connects — but a courtesy check that disagrees with
    // the authority is worse than none: it accepts URLs that will never deliver.
    let policy = relay_domain::url_guard::Policy::from_env();
    tracing::info!(?policy, "endpoint policy");
    // Kept in step with the dispatcher's payload cap. Ingest accepting more than
    // delivery will send would mean answering `202` to events that are guaranteed to
    // fail permanently.
    let max_body_bytes = match std::env::var("RELAY_MAX_PAYLOAD_BYTES") {
        Ok(v) => v.parse().map_err(|_| {
            anyhow::anyhow!("RELAY_MAX_PAYLOAD_BYTES must be a byte count, got {v:?}")
        })?,
        Err(_) => relay_api::extract::MAX_BODY_BYTES,
    };

    // The customer's whole deployment has to fit inside this. Shorter than the time
    // it takes to notice a rotation, change a config and roll a fleet means the
    // window expires mid-migration — the outage the overlap exists to prevent.
    let secret_overlap = match std::env::var("RELAY_SECRET_OVERLAP_SECS") {
        Ok(v) => std::time::Duration::from_secs(v.parse().map_err(|_| {
            anyhow::anyhow!("RELAY_SECRET_OVERLAP_SECS must be a number of seconds, got {v:?}")
        })?),
        Err(_) => relay_api::DEFAULT_SECRET_OVERLAP,
    };

    let state = AppState {
        store,
        policy,
        max_body_bytes,
        secret_overlap,
        transports: Default::default(),
        // How long the API waits before deciding the dispatcher has stopped
        // dispatching. See `readiness` for why lateness rather than depth.
        readiness: relay_api::readiness::Thresholds::from_env(),
    };

    // Logged and carried on rather than fatal. An API that refuses to accept events
    // because it could not install a counter has turned a missing dashboard into a
    // lost webhook.
    let app = match relay_metrics::Exporter::install() {
        // Deliberately no queue gauges here. Those describe rows in a database this
        // process shares with the dispatcher, and the dispatcher is the one that
        // reports them — two reporters would double every panel that sums across
        // instances.
        Ok(exporter) => router_with_metrics(state, exporter),
        Err(e) => {
            tracing::error!(error = %e, "metrics recorder could not be installed");
            router(state)
        }
    };

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, "relay-api listening");
    axum::serve(listener, app).await?;
    Ok(())
}
