use relay_api::{AppState, router};
use relay_store::Store;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://relay:relay@localhost:5433/relay".into());
    let bind = std::env::var("RELAY_API_BIND").unwrap_or_else(|_| "0.0.0.0:8080".into());

    let store = Store::connect(&database_url, 5).await?;
    store.migrate().await?;

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, "relay-api listening");
    axum::serve(listener, router(AppState { store })).await?;
    Ok(())
}
