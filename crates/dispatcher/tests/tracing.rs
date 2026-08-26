//! One delivery, one span, and nothing secret in it.
//!
//! The reason to assert on the rendered JSON rather than on a test-only recorder is
//! the same reason the metrics tests assert on the rendered scrape: the JSON is what
//! a production deployment actually emits, so a span that never reaches it is
//! exactly the bug worth catching. A recorder assertion would pass for a field no
//! log aggregator could ever index.
//!
//! Requires Postgres: `docker compose up -d`.

use std::{
    io::Write,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use relay_dispatcher::{Limits, Pool, PoolConfig, RequestLimits, SenderConfig};
use relay_domain::{backoff::Backoff, url_guard::Policy};
use relay_store::Store;
use relay_testkit::Receiver;
use sqlx::PgPool;
use tracing_subscriber::fmt::MakeWriter;
use uuid::Uuid;

/// The secret every endpoint in this file is registered with. It must never appear
/// in the captured output, so it is distinctive enough that a substring search for
/// it cannot match anything else.
const SECRET: &str = "whsec_this_must_never_be_logged_0d1a2b3c";

// ------------------------------------------------------------------- capturing

/// A writer the subscriber can hand lines to, that a test can read back.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Captured {
    /// How much has been written so far.
    ///
    /// The subscriber is installed once and the buffer is never cleared, so a test
    /// that read the whole thing would see every earlier test's deliveries too — and
    /// "the first `delivered` event" would belong to somebody else.
    fn mark(&self) -> usize {
        self.0.lock().expect("not poisoned").len()
    }

    /// Everything written since `mark`, as text.
    fn text_since(&self, mark: usize) -> String {
        let buf = self.0.lock().expect("not poisoned");
        String::from_utf8_lossy(&buf[mark.min(buf.len())..]).into_owned()
    }

    /// The JSON events written since `mark`.
    fn events_since(&self, mark: usize) -> Vec<serde_json::Value> {
        self.text_since(mark)
            .lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect()
    }
}

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("not poisoned").extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Captured {
    type Writer = Captured;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// The one subscriber this process gets. Global, so the tests take turns.
static CAPTURED: OnceLock<Captured> = OnceLock::new();

/// Spans are ambient per thread and the subscriber is per process, so two tests
/// logging at once interleave into one buffer with no way to tell them apart.
static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Install the capturing subscriber and return a handle to what it collects.
///
/// The configuration is deliberately the same one `relay_metrics::logging` installs
/// for a container: JSON, with the span list attached to every event. Testing a
/// different configuration would prove nothing about the one that ships.
fn capture() -> Captured {
    CAPTURED
        .get_or_init(|| {
            let captured = Captured::default();
            tracing_subscriber::fmt()
                .json()
                .with_current_span(true)
                .with_span_list(true)
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
                .with_max_level(tracing::Level::DEBUG)
                .with_writer(captured.clone())
                .init();
            captured
        })
        .clone()
}

// ------------------------------------------------------------------- the setup

fn config() -> SenderConfig {
    SenderConfig {
        backoff: Backoff {
            base: Duration::from_millis(5),
            cap: Duration::from_millis(20),
            max_attempts: 5,
            retry_after_cap: Duration::from_secs(300),
        },
        policy: Policy::permissive(),
        request: RequestLimits::default(),
        rate_limit: false,
        limits: Limits {
            max_in_flight: 1024,
            per_endpoint: 1024,
        },
        breaker: None,
    }
}

fn pool_config() -> PoolConfig {
    PoolConfig {
        workers: 4,
        batch_size: 4,
        idle_poll: Duration::from_millis(5),
        shutdown_deadline: Duration::from_secs(5),
    }
}

async fn seed(store: &Store, receiver: &Receiver, path: &str, n: usize) -> Vec<Uuid> {
    let addr = receiver.spawn().await;
    let event_type = format!("tr.{}", Uuid::new_v4());
    store
        .create_endpoint(
            &format!("http://{addr}{path}"),
            SECRET,
            std::slice::from_ref(&event_type),
        )
        .await
        .expect("endpoint");

    let mut ids = Vec::new();
    for _ in 0..n {
        ids.extend(
            store
                .insert_event_and_fan_out(&event_type, br#"{"hello":"world"}"#)
                .await
                .expect("insert")
                .delivery_ids,
        );
    }
    ids
}

/// The span named `name` from an event's ancestor list, if it has one.
fn span_named<'a>(event: &'a serde_json::Value, name: &str) -> Option<&'a serde_json::Value> {
    event
        .get("spans")?
        .as_array()?
        .iter()
        .find(|s| s.get("name").and_then(|n| n.as_str()) == Some(name))
}

/// The event whose message is `message`.
fn event_with(events: &[serde_json::Value], message: &str) -> serde_json::Value {
    events
        .iter()
        .find(|e| {
            e.get("fields")
                .and_then(|f| f.get("message"))
                .and_then(|m| m.as_str())
                == Some(message)
        })
        .unwrap_or_else(|| panic!("no {message:?} event was logged"))
        .clone()
}

// ------------------------------------------------------------------- the tests

#[sqlx::test(migrations = "../store/migrations")]
async fn a_deliverys_whole_path_is_reconstructable_from_one_span(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let captured = capture();
    let mark = captured.mark();
    let store = Store::from_pool(pool);
    let receiver = Receiver::new(SECRET);
    let ids = seed(&store, &receiver, "/verify", 1).await;

    Pool::with_config(store, pool_config(), config())
        .run_once()
        .await
        .expect("run");

    let events = captured.events_since(mark);
    let delivered = event_with(&events, "delivered");

    // Every stage the delivery passed through, named. Without these the log says a
    // delivery happened and nothing about where the time went.
    for stage in ["gate", "send", "persist"] {
        // Each stage reports when it closes, carrying how long it was busy. That
        // close line is what turns "the delivery took nine seconds" into "eight of
        // them were in `send`".
        let closed = events.iter().any(|e| {
            e.get("span")
                .and_then(|s| s.get("name"))
                .and_then(|n| n.as_str())
                == Some(stage)
                && e.get("fields")
                    .and_then(|f| f.get("message"))
                    .and_then(|m| m.as_str())
                    == Some("close")
        });
        assert!(closed, "the {stage:?} stage never reported");
    }

    // The identifying fields, on the span rather than repeated on every line.
    let delivery = span_named(&delivered, "delivery").expect("the delivery span");
    assert_eq!(
        delivery.get("delivery_id").and_then(|v| v.as_str()),
        Some(ids[0].to_string().as_str())
    );
    assert_eq!(delivery.get("attempt").and_then(|v| v.as_u64()), Some(0));
    // Recorded at the end, on the span that was opened at the start — which is what
    // makes "how did this delivery end" answerable without joining two log lines.
    assert_eq!(
        delivery.get("outcome").and_then(|v| v.as_str()),
        Some("success")
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_spawned_delivery_is_parented_to_the_claim_that_produced_it(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let captured = capture();
    let mark = captured.mark();
    let store = Store::from_pool(pool);
    let receiver = Receiver::new(SECRET);
    seed(&store, &receiver, "/verify", 3).await;

    Pool::with_config(store, pool_config(), config())
        .run_once()
        .await
        .expect("run");

    let events = captured.events_since(mark);
    let delivered = event_with(&events, "delivered");

    // The gotcha this exists to catch. A span is ambient to the current thread and
    // `spawn` moves the future to another one, so a task spawned without an explicit
    // `.instrument()` loses the whole chain: the events still appear, with no
    // delivery id and no parent, and nothing afterwards can say which delivery they
    // described. That failure is invisible — the logs look fine until the day
    // somebody needs them.
    assert!(
        span_named(&delivered, "delivery").is_some(),
        "the spawned task lost its delivery span"
    );
    assert!(
        span_named(&delivered, "batch").is_some(),
        "the spawned task was orphaned from the claim that produced it"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn the_signing_secret_never_reaches_the_log(pool: PgPool) {
    let _guard = ONE_AT_A_TIME.lock().await;
    let captured = capture();
    let mark = captured.mark();
    let store = Store::from_pool(pool);
    let receiver = Receiver::new(SECRET);
    // Three destinations, so the failing and refused paths are exercised as well as
    // the happy one. A secret leaks on the path nobody thought about, which is
    // always an error path.
    seed(&store, &receiver, "/verify", 1).await;
    seed(&store, &receiver, "/always500", 1).await;
    seed(&store, &receiver, "/nope", 1).await;

    let pool_ = Pool::with_config(store, pool_config(), config());
    pool_.run_once().await.expect("run");

    let text = captured.text_since(mark);
    assert!(
        !text.contains(SECRET),
        "the signing secret was written to the log"
    );
    // Not a weaker check than it looks: the prefix is what makes a leaked secret
    // greppable, so if any part of one reached the log this finds it.
    assert!(
        !text.contains("whsec_"),
        "something secret-shaped was logged"
    );
}

#[sqlx::test(migrations = "../store/migrations")]
async fn a_claimed_row_cannot_print_its_own_secret(pool: PgPool) {
    let store = Store::from_pool(pool);
    let receiver = Receiver::new(SECRET);
    seed(&store, &receiver, "/verify", 1).await;

    let pending = store
        .next_pending_delivery()
        .await
        .expect("query")
        .expect("a pending delivery");

    // The structural half of the guarantee, and the one that survives the next
    // person adding a log line in a hurry. `Debug` is written by hand precisely so
    // that a `?pending` cannot leak a secret, however careless the call site.
    let printed = format!("{pending:?}");
    assert!(!printed.contains(SECRET), "Debug printed the secret");
    assert!(printed.contains("<redacted>"));
    // Still useful for debugging, which is the point of redacting rather than
    // omitting.
    assert!(printed.contains(&pending.delivery_id.to_string()));

    let endpoint = store
        .get_endpoint(pending.endpoint_id)
        .await
        .expect("endpoint");
    let printed = format!("{endpoint:?}");
    assert!(!printed.contains(SECRET), "Debug printed the secret");
    assert!(printed.contains("<redacted>"));
}
