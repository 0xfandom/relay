//! Whether Relay is ready to be sent traffic.
//!
//! `/healthz` and `/readyz` answer two different questions and conflating them
//! causes a specific outage: an orchestrator restarts what fails liveness, so a
//! liveness probe that also checks a dependency turns "the database blinked" into
//! "every replica restarted at once". `/healthz` therefore stays as it is — this
//! process is running — and everything that can be temporarily and legitimately
//! false lives here.
//!
//! Three facts make up the answer, and the interesting one is the third.
//!
//! 1. The database answers. Nothing works without it.
//! 2. The dispatcher reported recently. A heartbeat, so an idle system with a dead
//!    dispatcher is not mistaken for a healthy one.
//! 3. The queue is draining. The heartbeat only proves a process is looping; it
//!    proves nothing about whether that loop is achieving anything. A dispatcher
//!    wedged on a poisoned row, or one whose workers are all parked on a hung
//!    endpoint, goes on beating happily while the queue climbs.
//!
//! The third check is the one the milestone asks for, and it is worth being precise
//! about what it measures. Not queue *depth*: depth is large and harmless while a
//! burst drains, and three rows stuck for an hour is a catastrophe that depth
//! reports as "3". What it measures is *lateness* — how far past its due time the
//! oldest pending delivery is. Every deliberate wait in Relay (a backoff, a rate
//! limit, a breaker, a concurrency cap) moves the row's due time forward, so a
//! delivery is late only when nothing has come to collect it.

use std::time::Duration;

use serde::Serialize;

/// The component name the dispatcher heartbeats under.
///
/// Re-exported from the store rather than declared again here: the writer and the
/// reader are different binaries, and two copies of a string that must match is a
/// typo waiting to become a permanent false outage.
pub use relay_store::HEARTBEAT_DISPATCHER as DISPATCHER;

/// How tolerant readiness is before it starts failing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Thresholds {
    /// How old the dispatcher's heartbeat may be.
    ///
    /// Wants to be several of the dispatcher's beat intervals, not one. A single
    /// interval means one slow write — a busy connection pool, a checkpoint — reads
    /// as a dead process, and a readiness endpoint that flaps under load is worse
    /// than none: it removes capacity at the exact moment capacity is short.
    pub heartbeat_max_age: Duration,
    /// How far behind its due time the oldest pending delivery may be.
    ///
    /// Generous on purpose. This has to sit above the worst honest lateness — a
    /// burst arriving faster than the workers drain it — or a busy Relay declares
    /// itself unready for doing its job well.
    pub max_lateness: Duration,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            // Four missed beats at the dispatcher's default interval of five seconds.
            heartbeat_max_age: Duration::from_secs(20),
            max_lateness: Duration::from_secs(60),
        }
    }
}

impl Thresholds {
    pub fn from_env() -> Self {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// The parsing, separated from the environment so it can be tested.
    ///
    /// An unparseable value falls back to the default rather than failing the
    /// process. The alternative — refusing to start — makes a typo in an optional
    /// tuning knob into a total outage.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let d = Self::default();
        let secs = |key: &str, fallback: Duration| {
            get(key)
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or(fallback)
        };
        Self {
            heartbeat_max_age: secs("RELAY_HEARTBEAT_MAX_AGE_SECS", d.heartbeat_max_age),
            max_lateness: secs("RELAY_MAX_LATENESS_SECS", d.max_lateness),
        }
    }
}

/// What was observed, before any judgement is applied.
///
/// Split this way because the database is a precondition for the other two, not a
/// peer of them: if the connection is down there is no heartbeat to read and no
/// queue to measure, and reporting those as *failing* would be a fabrication. They
/// were not checked.
#[derive(Debug, Clone, PartialEq)]
pub enum Facts {
    DatabaseDown(String),
    Live {
        /// Seconds since the dispatcher last reported; `None` if it never has.
        heartbeat_age_secs: Option<f64>,
        /// Seconds past due for the oldest pending delivery. Negative while the
        /// oldest thing in the queue is not due yet, `None` when it is empty.
        lateness_secs: Option<f64>,
    },
}

/// One check's verdict.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Check {
    Pass { detail: String },
    Fail { detail: String },
    /// Not run, because something it depends on already failed.
    Skipped { detail: String },
}

impl Check {
    fn failed(&self) -> bool {
        // A skipped check is not a pass, but it is not this check's failure either.
        // The thing it depended on already reported, and counting both would report
        // one outage as three.
        matches!(self, Check::Fail { .. })
    }
}

/// The whole answer, in the shape it is served in.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Readiness {
    pub ready: bool,
    pub database: Check,
    pub dispatcher: Check,
    pub queue: Check,
}

/// Turn observations into a verdict.
///
/// Deliberately a pure function over plain numbers. Every rule below is a decision
/// about when to take a production system out of rotation, which makes it exactly
/// the kind of logic that should be provable without a database, a dispatcher or a
/// clock.
pub fn evaluate(facts: &Facts, thresholds: Thresholds) -> Readiness {
    let (heartbeat_age_secs, lateness_secs) = match facts {
        Facts::DatabaseDown(detail) => {
            let skipped = Check::Skipped {
                detail: "not checked: the database is unreachable".into(),
            };
            return Readiness {
                ready: false,
                database: Check::Fail {
                    detail: detail.clone(),
                },
                dispatcher: skipped.clone(),
                queue: skipped,
            };
        }
        Facts::Live {
            heartbeat_age_secs,
            lateness_secs,
        } => (*heartbeat_age_secs, *lateness_secs),
    };

    let max_age = thresholds.heartbeat_max_age.as_secs_f64();
    let dispatcher = match heartbeat_age_secs {
        // Never reported. Treated as stale, not as fine: this is what a cold start
        // looks like before the dispatcher is up, and it is also what a dispatcher
        // that has never once managed to write looks like.
        None => Check::Fail {
            detail: "the dispatcher has never reported".into(),
        },
        Some(age) if age > max_age => Check::Fail {
            detail: format!("last reported {age:.1}s ago, over the {max_age:.0}s limit"),
        },
        Some(age) => Check::Pass {
            detail: format!("last reported {age:.1}s ago"),
        },
    };

    let max_lateness = thresholds.max_lateness.as_secs_f64();
    let queue = match lateness_secs {
        None => Check::Pass {
            detail: "nothing pending".into(),
        },
        // Negative lateness is the ordinary state of a healthy queue: the oldest
        // thing in it is a retry scheduled for later. Reported as draining, because
        // there is nothing to drain yet.
        Some(late) if late <= 0.0 => Check::Pass {
            detail: "nothing due".into(),
        },
        Some(late) if late > max_lateness => Check::Fail {
            detail: format!(
                "the oldest pending delivery is {late:.1}s past due, over the \
                 {max_lateness:.0}s limit"
            ),
        },
        Some(late) => Check::Pass {
            detail: format!("the oldest pending delivery is {late:.1}s past due"),
        },
    };

    Readiness {
        ready: !dispatcher.failed() && !queue.failed(),
        database: Check::Pass {
            detail: "reachable".into(),
        },
        dispatcher,
        queue,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live(heartbeat: Option<f64>, lateness: Option<f64>) -> Facts {
        Facts::Live {
            heartbeat_age_secs: heartbeat,
            lateness_secs: lateness,
        }
    }

    #[test]
    fn an_idle_system_with_a_live_dispatcher_is_ready() {
        let r = evaluate(&live(Some(2.0), None), Thresholds::default());
        assert!(r.ready);
    }

    #[test]
    fn a_dead_dispatcher_is_caught_even_when_the_queue_is_empty() {
        // The case a queue-only check cannot see. Nothing is late because nothing is
        // pending, and the system is nonetheless incapable of delivering anything.
        let r = evaluate(&live(Some(600.0), None), Thresholds::default());
        assert!(!r.ready);
        assert!(r.dispatcher.failed());
        assert!(!r.queue.failed());
    }

    #[test]
    fn a_beating_dispatcher_that_delivers_nothing_is_caught() {
        // The mirror image, and the reason the milestone asks for more than liveness:
        // the loop is running and the queue is not moving.
        let r = evaluate(&live(Some(1.0), Some(300.0)), Thresholds::default());
        assert!(!r.ready);
        assert!(!r.dispatcher.failed());
        assert!(r.queue.failed());
    }

    #[test]
    fn a_dispatcher_that_has_never_reported_is_not_ready() {
        let r = evaluate(&live(None, None), Thresholds::default());
        assert!(!r.ready);
    }

    #[test]
    fn a_queue_full_of_work_that_is_not_due_yet_is_ready() {
        // A thousand retries all scheduled for an hour from now. Depth is enormous,
        // lateness is negative, and there is nothing wrong.
        let r = evaluate(&live(Some(1.0), Some(-3600.0)), Thresholds::default());
        assert!(r.ready);
    }

    #[test]
    fn lateness_under_the_limit_is_ready() {
        // A burst being worked through. Late, but not late enough to mean stalled.
        let r = evaluate(&live(Some(1.0), Some(5.0)), Thresholds::default());
        assert!(r.ready);
    }

    #[test]
    fn a_down_database_skips_the_checks_it_would_have_made_up() {
        let r = evaluate(&Facts::DatabaseDown("connection refused".into()), Thresholds::default());
        assert!(!r.ready);
        assert!(r.database.failed());
        assert!(matches!(r.dispatcher, Check::Skipped { .. }));
        assert!(matches!(r.queue, Check::Skipped { .. }));
    }

    #[test]
    fn thresholds_come_from_the_environment() {
        let t = Thresholds::from_lookup(|k| match k {
            "RELAY_HEARTBEAT_MAX_AGE_SECS" => Some("5".into()),
            "RELAY_MAX_LATENESS_SECS" => Some("7".into()),
            _ => None,
        });
        assert_eq!(t.heartbeat_max_age, Duration::from_secs(5));
        assert_eq!(t.max_lateness, Duration::from_secs(7));
    }

    #[test]
    fn a_nonsense_threshold_falls_back_rather_than_disabling_the_check() {
        // The dangerous alternative is parsing "thirty" as zero, which makes every
        // check fail, or as infinity, which makes every check pass.
        let t = Thresholds::from_lookup(|k| match k {
            "RELAY_MAX_LATENESS_SECS" => Some("thirty".into()),
            _ => None,
        });
        assert_eq!(t.max_lateness, Thresholds::default().max_lateness);
    }
}
