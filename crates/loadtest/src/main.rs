//! Measures what Relay actually does, and finds the thing that limits it.
//!
//! # What is being measured
//!
//! Sustained load, not a backlog drain. Those are different numbers and the
//! difference matters: seeding a hundred thousand rows and timing how long they take
//! to clear measures peak drain rate with a queue that is never empty, which is the
//! most flattering possible framing. It also makes the latency figure meaningless —
//! the last row waits for the whole run, so p99 is a function of how many rows were
//! seeded rather than of anything about the system.
//!
//! So the producer runs at a fixed rate *while* the dispatcher drains, exactly as
//! traffic would, and the question is whether the dispatcher keeps up.
//!
//! # What latency means here
//!
//! `(attempt.at - delivery.created_at) - attempt.latency_ms`.
//!
//! That is the time a delivery spent inside Relay: waiting to be claimed, being
//! gated, being persisted. `latency_ms` is the receiver's own response time and is
//! subtracted, because a slow receiver is not a Relay latency problem and including
//! it would let a fast receiver flatter the result.
//!
//! # Why the rate limiter and breaker are off
//!
//! An endpoint's default rate is 10 deliveries per second. Left on, this whole
//! program would measure the rate limiter — correctly, and uselessly. The breaker is
//! off for the same reason: it is not what is being measured.
//!
//! # Usage
//!
//!   DATABASE_URL=... cargo run --release -p relay-loadtest
//!   LOADTEST_MODE=sweep cargo run --release -p relay-loadtest
//!
//! Release mode is not optional. A debug build measures the absence of optimisation.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use relay_dispatcher::{Limits, Pool, PoolConfig, RequestLimits, SenderConfig};
use relay_domain::{backoff::Backoff, rate_limit::Rate, url_guard::Policy};
use relay_store::Store;
use relay_testkit::Receiver;
use tokio_util::sync::CancellationToken;

/// One configuration's result.
#[derive(Debug, Clone)]
struct Run {
    workers: usize,
    batch: usize,
    connections: u32,
    produced: u64,
    delivered: i64,
    lost: i64,
    /// Wall clock the rate was computed over. Printed, so a suspicious rate can be
    /// checked against the window it came from rather than taken on faith.
    seconds: f64,
    per_second: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://relay:relay@localhost:5433/relay".into());

    let rate: u64 = env("LOADTEST_RATE", 1_000);
    let seconds: u64 = env("LOADTEST_SECONDS", 30);
    let endpoints: usize = env("LOADTEST_ENDPOINTS", 8);

    match std::env::var("LOADTEST_MODE").as_deref() {
        Ok("sweep") => sweep(&database_url, rate, seconds, endpoints).await,
        // Two halves of one measurement against a Relay that is not in this process:
        // fill the queue, let the real dispatcher empty it, then read the result out
        // of the attempt log. Separate commands because the thing being measured is
        // started and stopped by `docker compose`, not by this program.
        Ok("seed") => {
            let store = Store::connect(&database_url, 16).await?;
            store.migrate().await?;
            reset(&store).await?;
            let receiver =
                std::env::var("LOADTEST_RECEIVER").unwrap_or_else(|_| "receiver:9099".into());
            endpoints_for(&store, &receiver, endpoints).await?;
            let backlog: u64 = env("LOADTEST_BACKLOG", 30_000);
            let seeded = produce_now(&store, backlog, endpoints).await;
            println!("seeded {seeded} deliveries for {receiver}");
            Ok(())
        }
        // Paced production against a dispatcher this program did not start. The
        // sustained figure the acceptance criteria ask for, measured on the artifact
        // that actually ships rather than on a pool built inside the harness.
        Ok("feed") => {
            let store = Store::connect(&database_url, 16).await?;
            store.migrate().await?;
            reset(&store).await?;
            let receiver =
                std::env::var("LOADTEST_RECEIVER").unwrap_or_else(|_| "receiver:9099".into());
            endpoints_for(&store, &receiver, endpoints).await?;
            println!("feeding {rate}/s for {seconds}s to {receiver}");
            let produced = produce(&store, rate, seconds, endpoints).await;
            println!("produced {produced}");
            Ok(())
        }
        Ok("report") => {
            let store = Store::connect(&database_url, 4).await?;
            let m = measure(&store).await?;
            let stats = store.queue_stats().await?;
            println!(
                "delivered {} | pending {} | inflight {} | dead {} | p50 {:.0}ms p95 {:.0}ms \
                 p99 {:.0}ms max {:.0}ms",
                m.delivered,
                stats.pending,
                stats.inflight,
                stats.dead,
                m.p50_ms,
                m.p95_ms,
                m.p99_ms,
                m.max_ms
            );
            Ok(())
        }
        Ok("drain") => {
            let run = drain_only(
                &database_url,
                Config {
                    workers: env("RELAY_WORKERS", 32),
                    batch: env("RELAY_BATCH_SIZE", 32),
                    connections: env("RELAY_DB_CONNECTIONS", 8) as u32,
                    rate,
                    seconds,
                    endpoints,
                },
                env("LOADTEST_BACKLOG", 50_000u64),
            )
            .await?;
            println!();
            print_header();
            print_row(&run);
            Ok(())
        }
        _ => {
            let run = single(
                &database_url,
                Config {
                    workers: env("RELAY_WORKERS", 32),
                    batch: env("RELAY_BATCH_SIZE", 32),
                    connections: env("RELAY_DB_CONNECTIONS", 8) as u32,
                    rate,
                    seconds,
                    endpoints,
                },
            )
            .await?;
            println!();
            print_header();
            print_row(&run);
            println!();
            verdict(&run);
            Ok(())
        }
    }
}

struct Config {
    workers: usize,
    batch: usize,
    connections: u32,
    rate: u64,
    seconds: u64,
    endpoints: usize,
}

/// The grid.
///
/// Chosen to move one variable at a time around the defaults rather than to cover
/// every combination. A full cross product of three dimensions is dozens of runs of
/// half a minute each, and almost all of them answer a question nobody asked.
async fn sweep(
    database_url: &str,
    rate: u64,
    seconds: u64,
    endpoints: usize,
) -> anyhow::Result<()> {
    let mut grid = Vec::new();
    for workers in [8, 16, 32, 64, 128] {
        grid.push((workers, 32, 8));
    }
    for batch in [8, 16, 64, 128] {
        grid.push((32, batch, 8));
    }
    for connections in [2, 4, 16, 32] {
        grid.push((32, 32, connections));
    }

    let mut runs = Vec::new();
    print_header();
    for (workers, batch, connections) in grid {
        let run = single(
            database_url,
            Config {
                workers,
                batch,
                connections,
                rate,
                seconds,
                endpoints,
            },
        )
        .await?;
        print_row(&run);
        runs.push(run);
    }

    println!();
    let best = runs
        .iter()
        .filter(|r| r.lost == 0)
        .max_by(|a, b| a.per_second.total_cmp(&b.per_second));
    if let Some(b) = best {
        println!(
            "fastest without loss: {} workers, batch {}, {} connections -> {:.0}/s, p99 {:.0}ms",
            b.workers, b.batch, b.connections, b.per_second, b.p99_ms
        );
    }
    Ok(())
}

/// Register the endpoints the run delivers to.
///
/// Several rather than one, because the per-endpoint concurrency cap is a bulkhead:
/// a single endpoint would cap the whole run at `per_endpoint` in flight no matter
/// how many workers exist. Real traffic is spread across customers, and a benchmark
/// that ignores that is measuring the bulkhead.
async fn endpoints_for(store: &Store, addr: &str, count: usize) -> anyhow::Result<()> {
    for i in 0..count {
        let ep = store
            .create_endpoint(
                &format!("http://{addr}/sink"),
                "whsec_loadtest",
                &[format!("load.{i}")],
            )
            .await?;
        // Raised out of the way. The limiter is disabled in the sender config too;
        // this stops the stored rate mattering if that is ever changed.
        store
            .set_endpoint_rate(
                ep.id,
                Rate {
                    per_second: 1_000_000.0,
                    burst: 1_000_000.0,
                },
            )
            .await?;
    }
    Ok(())
}

fn pool_config(cfg: &Config) -> PoolConfig {
    PoolConfig {
        workers: cfg.workers,
        batch_size: cfg.batch,
        // Short, because an idle poll during a load test is a worker not working.
        idle_poll: Duration::from_millis(10),
        shutdown_deadline: Duration::from_secs(30),
    }
}

/// Shared by both modes, so a difference between their numbers cannot be a
/// difference in how they were configured.
fn sender_config(cfg: &Config) -> SenderConfig {
    SenderConfig {
        backoff: Backoff::default(),
        // The receiver is on loopback over plain HTTP, which the default policy
        // refuses. Nothing about the guard is under test here.
        policy: Policy::permissive(),
        rate_limit: false,
        limits: Limits {
            max_in_flight: cfg.workers * 2,
            per_endpoint: cfg.workers,
        },
        request: RequestLimits::default(),
        transports: Default::default(),
        breaker: None,
    }
}

/// Fill the queue first, then time how fast it empties with nobody else writing.
///
/// This exists because the sustained test measures two things at once. The producer
/// is a write workload of its own — one transaction per event, against the same
/// database the dispatcher is claiming from — so a sustained figure is the rate at
/// which *both* can proceed, and blaming the dispatcher for it would be exactly the
/// assumed bottleneck the issue warns about.
///
/// Drain rate is the send path's own ceiling. The gap between the two numbers is how
/// much the sustained figure was costing to contention.
async fn drain_only(database_url: &str, cfg: Config, backlog: u64) -> anyhow::Result<Run> {
    let store = Store::connect(database_url, cfg.connections).await?;
    store.migrate().await?;
    reset(&store).await?;

    let receiver = Receiver::new("whsec_loadtest");
    let addr = receiver.spawn().await;
    endpoints_for(&store, &addr.to_string(), cfg.endpoints).await?;

    // Seeded as fast as the machine will take it, and deliberately not timed: this is
    // setup, not measurement.
    let seeded = produce_now(&store, backlog, cfg.endpoints).await;

    let cancel = CancellationToken::new();
    let pool = Pool::with_config(store.clone(), pool_config(&cfg), sender_config(&cfg));
    let pool_loop = {
        let cancel = cancel.clone();
        tokio::spawn(async move { pool.run(cancel).await })
    };

    // The clock starts once there is a full queue and a running pool, so nothing but
    // sending is inside the measurement.
    let started = Instant::now();
    loop {
        let stats = store.queue_stats().await?;
        if stats.pending == 0 && stats.inflight == 0 {
            break;
        }
        if started.elapsed() > Duration::from_secs(300) {
            eprintln!("  drain deadline hit with {} pending", stats.pending);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let elapsed = started.elapsed().as_secs_f64();

    cancel.cancel();
    let _ = pool_loop.await;

    let m = measure(&store).await?;
    Ok(Run {
        workers: cfg.workers,
        batch: cfg.batch,
        connections: cfg.connections,
        produced: seeded,
        delivered: m.delivered,
        lost: seeded as i64 - m.delivered,
        seconds: elapsed,
        per_second: m.delivered as f64 / elapsed,
        // Latency is not meaningful here and is reported as measured rather than
        // quietly omitted: in a backlog drain every row's latency is a function of
        // where it sat in the queue, so p99 describes the size of the backlog.
        p50_ms: m.p50_ms,
        p95_ms: m.p95_ms,
        p99_ms: m.p99_ms,
        max_ms: m.max_ms,
    })
}

/// Insert a fixed number of events with no pacing at all.
async fn produce_now(store: &Store, total: u64, endpoints: usize) -> u64 {
    let sent = Arc::new(AtomicU64::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..total {
        let store = store.clone();
        let sent = sent.clone();
        let event_type = format!("load.{}", (i as usize) % endpoints);
        tasks.spawn(async move {
            if store
                .insert_event_and_fan_out(&event_type, br#"{"load":true}"#)
                .await
                .is_ok()
            {
                sent.fetch_add(1, Ordering::Relaxed);
            }
        });
        // Bounded, or a hundred thousand futures are alive at once and the harness
        // runs out of connections before the database runs out of anything.
        while tasks.len() >= 64 {
            let _ = tasks.join_next().await;
        }
    }
    while tasks.join_next().await.is_some() {}
    sent.load(Ordering::Relaxed)
}

async fn single(database_url: &str, cfg: Config) -> anyhow::Result<Run> {
    let store = Store::connect(database_url, cfg.connections).await?;
    store.migrate().await?;
    reset(&store).await?;

    // In this process, on loopback. A receiver on another machine would add its
    // network to every measurement, and the point is to find Relay's limit.
    let receiver = Receiver::new("whsec_loadtest");
    let addr = receiver.spawn().await;

    // Several endpoints rather than one, because the per-endpoint concurrency cap is
    // a bulkhead: one endpoint would cap the whole run at `per_endpoint` in flight no
    // matter how many workers exist. Real traffic is spread across customers, and a
    // benchmark that ignores that is measuring the bulkhead.
    endpoints_for(&store, &addr.to_string(), cfg.endpoints).await?;

    let cancel = CancellationToken::new();
    let pool = Pool::with_config(store.clone(), pool_config(&cfg), sender_config(&cfg));

    let pool_loop = {
        let cancel = cancel.clone();
        tokio::spawn(async move { pool.run(cancel).await })
    };

    let produced = produce(&store, cfg.rate, cfg.seconds, cfg.endpoints).await;

    // Let the queue finish, but do not wait forever: a configuration that cannot keep
    // up is a result, not a reason to hang.
    let started_drain = Instant::now();
    let drain_deadline = Duration::from_secs(60);
    loop {
        let stats = store.queue_stats().await?;
        if stats.pending == 0 && stats.inflight == 0 {
            break;
        }
        if started_drain.elapsed() > drain_deadline {
            eprintln!(
                "  drain deadline hit with {} pending, {} in flight",
                stats.pending, stats.inflight
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    cancel.cancel();
    let _ = pool_loop.await;

    let m = measure(&store).await?;
    // Wall clock spans production plus whatever drain ran past it. Dividing delivered
    // by this is the honest sustained rate; dividing by the production window alone
    // would credit the dispatcher with work it finished afterwards.
    let elapsed = cfg.seconds as f64 + started_drain.elapsed().as_secs_f64();

    Ok(Run {
        workers: cfg.workers,
        batch: cfg.batch,
        connections: cfg.connections,
        produced,
        delivered: m.delivered,
        lost: produced as i64 - m.delivered,
        seconds: elapsed,
        per_second: m.delivered as f64 / elapsed,
        p50_ms: m.p50_ms,
        p95_ms: m.p95_ms,
        p99_ms: m.p99_ms,
        max_ms: m.max_ms,
    })
}

/// Emit events at a fixed rate for a fixed time.
///
/// Paced in small ticks rather than one sleep per event: at a thousand a second the
/// per-event sleep would be a millisecond, which is below the timer's resolution, and
/// the producer would silently run as fast as it could.
async fn produce(store: &Store, rate: u64, seconds: u64, endpoints: usize) -> u64 {
    const TICK: Duration = Duration::from_millis(10);
    let per_tick = (rate / 100).max(1);
    let ticks = seconds * 100;
    let sent = Arc::new(AtomicU64::new(0));
    let mut ticker = tokio::time::interval(TICK);
    // If a tick is missed the producer should carry on at the target rate, not sprint
    // to catch up — a burst would measure a different thing than the one asked for.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut tasks = tokio::task::JoinSet::new();
    for tick in 0..ticks {
        ticker.tick().await;
        for _ in 0..per_tick {
            let store = store.clone();
            let sent = sent.clone();
            let event_type = format!("load.{}", (tick as usize) % endpoints);
            tasks.spawn(async move {
                if store
                    .insert_event_and_fan_out(&event_type, br#"{"load":true}"#)
                    .await
                    .is_ok()
                {
                    sent.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        // Keep the set from growing without bound over a long run.
        while tasks.try_join_next().is_some() {}
    }
    while tasks.join_next().await.is_some() {}
    sent.load(Ordering::Relaxed)
}

struct Measured {
    delivered: i64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
}

/// Read the answer out of the attempt log rather than out of the program's memory.
///
/// This is the part that makes "zero lost deliveries" a claim worth anything. A
/// counter incremented by the sender proves only that the sender believes it sent
/// something; the attempt log is what a customer would be shown, and it is written in
/// the same transaction that resolves the delivery.
async fn measure(store: &Store) -> anyhow::Result<Measured> {
    let row: (i64, Option<f64>, Option<f64>, Option<f64>, Option<f64>) = sqlx::query_as(
        "WITH latency AS (
             -- Cast: `EXTRACT(EPOCH ...)` is `numeric`, and so is the whole
             -- expression once it touches one. Reading that as `f64` fails at
             -- runtime rather than at compile time, which is the sort of thing a
             -- smoke run exists to find.
             SELECT (EXTRACT(EPOCH FROM (a.at - d.created_at)) * 1000.0
                        - a.latency_ms)::double precision AS ms
               FROM delivery_attempts a
               JOIN deliveries d ON d.id = a.delivery_id
              WHERE a.outcome_class = 'success'
         )
         SELECT count(*)::bigint,
                percentile_disc(0.50) WITHIN GROUP (ORDER BY ms),
                percentile_disc(0.95) WITHIN GROUP (ORDER BY ms),
                percentile_disc(0.99) WITHIN GROUP (ORDER BY ms),
                max(ms)
           FROM latency",
    )
    .fetch_one(store.pool())
    .await?;

    Ok(Measured {
        delivered: row.0,
        p50_ms: row.1.unwrap_or(0.0),
        p95_ms: row.2.unwrap_or(0.0),
        p99_ms: row.3.unwrap_or(0.0),
        max_ms: row.4.unwrap_or(0.0),
    })
}

/// Empty the tables this measures, so one run cannot read another's rows.
async fn reset(store: &Store) -> anyhow::Result<()> {
    // `TRUNCATE` rather than `DELETE`, and cascading, because the attempt log is
    // partitioned and a row-by-row delete across partitions is slow enough to show up
    // as setup time between sweep runs.
    sqlx::query("TRUNCATE deliveries, events, endpoints, delivery_attempts CASCADE")
        .execute(store.pool())
        .await?;
    Ok(())
}

fn print_header() {
    println!(
        "{:>7} {:>6} {:>6} {:>9} {:>9} {:>6} {:>7} {:>9} {:>8} {:>8} {:>8} {:>9}",
        "workers",
        "batch",
        "conns",
        "produced",
        "delivered",
        "lost",
        "secs",
        "per sec",
        "p50 ms",
        "p95 ms",
        "p99 ms",
        "max ms"
    );
}

fn print_row(r: &Run) {
    println!(
        "{:>7} {:>6} {:>6} {:>9} {:>9} {:>6} {:>7.1} {:>9.0} {:>8.0} {:>8.0} {:>8.0} {:>9.0}",
        r.workers,
        r.batch,
        r.connections,
        r.produced,
        r.delivered,
        r.lost,
        r.seconds,
        r.per_second,
        r.p50_ms,
        r.p95_ms,
        r.p99_ms,
        r.max_ms
    );
}

/// State plainly whether the acceptance criteria were met.
fn verdict(r: &Run) {
    let throughput = r.per_second >= 1_000.0;
    let latency = r.p99_ms < 5_000.0;
    let complete = r.lost == 0;
    println!(
        "1,000 deliveries/sec sustained : {}  ({:.0}/s)",
        pass(throughput),
        r.per_second
    );
    println!(
        "p99 under 5s excluding receiver: {}  ({:.0}ms)",
        pass(latency),
        r.p99_ms
    );
    println!(
        "zero lost deliveries           : {}  ({} produced, {} in the attempt log)",
        pass(complete),
        r.produced,
        r.delivered
    );
}

fn pass(ok: bool) -> &'static str {
    if ok { "PASS" } else { "FAIL" }
}

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
