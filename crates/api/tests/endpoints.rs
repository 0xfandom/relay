//! What registration will accept.
//!
//! Registration is a courtesy check — the authority is the send path, which resolves
//! the address at the moment it connects, because a domain that is public today can
//! be repointed tomorrow. But a courtesy check that disagrees with the authority is
//! worse than none: it accepts URLs that will never deliver, and the caller finds
//! out from the dead letter queue instead of from the request they could have fixed.
//!
//! So both processes build the policy from the same variables, and these tests are
//! about the half a URL can be judged on immediately: its scheme and its port.
//!
//! Requires Postgres: `docker compose up -d`.

use std::net::SocketAddr;

use relay_api::{AppState, router};
use relay_store::Store;
use serde_json::Value;
use sqlx::PgPool;

async fn serve(state: AppState) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(state);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

async fn register(addr: SocketAddr, url: &str) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/v1/endpoints"))
        .json(&serde_json::json!({ "url": url }))
        .send()
        .await
        .expect("register");
    let status = resp.status().as_u16();
    (status, resp.json().await.unwrap_or(Value::Null))
}

fn error(body: &Value) -> String {
    body["error"].as_str().unwrap_or_default().to_string()
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_production_policy_requires_tls(pool: PgPool) {
    let addr = serve(AppState::new(Store::from_pool(pool))).await;

    let (status, body) = register(addr, "http://example.com/hook").await;
    assert_eq!(status, 400);
    // Refused now, at the point the caller can still fix it, rather than accepted
    // and then dead-lettered by a process they cannot see.
    assert!(error(&body).contains("https is required"), "got {body}");

    let (status, _) = register(addr, "https://example.com/hook").await;
    assert_eq!(status, 201);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_production_policy_restricts_the_port(pool: PgPool) {
    let addr = serve(AppState::new(Store::from_pool(pool))).await;

    let (status, body) = register(addr, "https://example.com:6379/hook").await;
    assert_eq!(status, 400);
    assert!(error(&body).contains("port 6379"), "got {body}");
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_scheme_no_receiver_speaks_is_refused_either_way(pool: PgPool) {
    // `file://` is not a webhook receiver and does read local files, so it is
    // refused under every policy — the development switch relaxes where deliveries
    // may go, not what they may be.
    for state in [
        AppState::new(Store::from_pool(pool.clone())),
        AppState::permissive(Store::from_pool(pool.clone())),
    ] {
        let addr = serve(state).await;
        let (status, body) = register(addr, "file:///etc/passwd").await;
        assert_eq!(status, 400);
        assert!(error(&body).contains("not http or https"), "got {body}");
    }
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_development_policy_accepts_a_loopback_receiver(pool: PgPool) {
    let addr = serve(AppState::permissive(Store::from_pool(pool))).await;

    // The local demo: plain HTTP, loopback, and whatever port the testkit was given.
    // One switch has to make all three work, or somebody sets it in production to
    // make an error go away.
    let (status, _) = register(addr, "http://127.0.0.1:9099/verify").await;
    assert_eq!(status, 201);
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_url_with_no_authority_is_refused(pool: PgPool) {
    let addr = serve(AppState::permissive(Store::from_pool(pool))).await;

    // A port and no host at all. The parser rejects it, and that rejection has to
    // reach the caller as a `400` rather than as a `500` from somewhere deeper.
    let (status, body) = register(addr, "https://:443/x").await;
    assert_eq!(status, 400);
    assert!(error(&body).contains("invalid url"), "got {body}");

    // The trap next door, asserted so nobody "fixes" it later: `https:///hook` looks
    // hostless and is not. The parser collapses the slashes and reads `hook` as the
    // host, so this is a well-formed URL naming a host that happens not to exist. It
    // is accepted here and refused at send time, when it resolves to nothing — which
    // is the correct division of labour, since only the send path can know.
    let (status, _) = register(addr, "https:///hook").await;
    assert_eq!(status, 201);
}
