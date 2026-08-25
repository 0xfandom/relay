//! When to stop knocking.
//!
//! Retries and rate limits both assume the endpoint is worth talking to. A circuit
//! breaker is the case where it is not: the server has been down for an hour, every
//! delivery to it will time out, and each one costs a worker for the full request
//! timeout before learning what the last thousand already established.
//!
//! Three states, and the middle one is the interesting one:
//!
//! ```text
//!            threshold consecutive failures
//!   Closed ────────────────────────────────▶ Open
//!     ▲                                       │
//!     │ probe succeeds          cooldown expires
//!     │                                       ▼
//!     └──────────────── HalfOpen ◀────────────┘
//!                          │
//!                          └── probe fails ──▶ Open (longer cooldown)
//! ```
//!
//! `Closed` is normal. `Open` means stop — the endpoint is presumed dead and
//! deliveries wait. `HalfOpen` means one delivery is allowed through to find out
//! whether it is back. Without that third state a breaker that opens never closes,
//! because nothing is ever tried again.
//!
//! ## What counts as evidence
//!
//! Not every failure says something about the server's health, and feeding the wrong
//! ones in trips on the wrong signal. A stream of `404`s is a misconfigured URL and a
//! `429` is us going too fast — in both cases the server is *up and answering*.
//! Opening the breaker there hides a problem that needs a person, and stops
//! deliveries to a destination that was working fine.
//!
//! So the question is not "did this attempt succeed" but "did the endpoint answer".
//! See [`Health`].
//!
//! ## No clock and no storage
//!
//! Like [`crate::backoff`] and [`crate::rate_limit`], everything here is a function
//! from values to values. The caller supplies the current state and gets the next one
//! back, which is what lets every state and event pair be tested exhaustively — and
//! what lets the state live in Postgres, shared across processes, rather than in one
//! process's memory.

use std::time::Duration;

use crate::outcome::Transport;

/// What one attempt says about the endpoint's health.
///
/// The distinction is "did the server answer", not "did we get what we wanted".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Health {
    /// The endpoint answered. It is alive, whatever it said — a `404` is a wrong
    /// path on a working server and a `429` is a working server telling us to slow
    /// down. Neither is a reason to stop talking to it.
    Alive,
    /// No answer at all, or the server itself reported a fault. A connection
    /// refused, a timeout, or a `5xx`: the endpoint is not serving requests.
    Failing,
    /// No request was made, so there is nothing to learn. A URL that will not parse
    /// or an address that was refused never reached the network.
    Unknown,
}

/// Read an attempt's outcome as evidence about the endpoint.
///
/// `status` is the HTTP status if one came back; `transport` is the network-level
/// failure if one occurred. Exactly one is normally present.
pub fn health(status: Option<u16>, transport: Option<Transport>) -> Health {
    if let Some(status) = status {
        // Anything the server actually sent proves it is running. `5xx` is the
        // exception: that is the server reporting its own fault.
        return if (500..600).contains(&status) {
            Health::Failing
        } else {
            Health::Alive
        };
    }
    match transport {
        // Nothing answered.
        Some(Transport::Timeout | Transport::Connect | Transport::Other) => Health::Failing,
        // A URL that will not parse. We never left the process.
        Some(Transport::Invalid) => Health::Unknown,
        None => Health::Unknown,
    }
}

/// Where a breaker is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Normal. Deliveries go out.
    Closed,
    /// Presumed dead. Deliveries wait for the cooldown to expire.
    Open,
    /// One delivery is allowed through to find out whether it is back.
    HalfOpen,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "closed" => Some(Self::Closed),
            "open" => Some(Self::Open),
            "half_open" => Some(Self::HalfOpen),
            _ => None,
        }
    }
}

/// What happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// An attempt finished and this is what it says about the endpoint.
    Attempted(Health),
    /// The cooldown ran out and a probe may now be issued.
    CooldownExpired,
}

/// The breaker's stored state for one endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Breaker {
    pub state: State,
    /// Consecutive [`Health::Failing`] attempts. Reset by any [`Health::Alive`].
    pub consecutive_failures: u32,
    /// How many times this breaker has opened without a successful probe in
    /// between. Drives how long the next cooldown is.
    pub trips: u32,
}

impl Default for Breaker {
    fn default() -> Self {
        Self {
            state: State::Closed,
            consecutive_failures: 0,
            trips: 0,
        }
    }
}

/// When to trip and how long to wait.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Policy {
    /// Consecutive failures that trip the breaker.
    ///
    /// Not one. A single timeout is noise — a dropped packet, a restarting process —
    /// and stopping delivery to an endpoint on that evidence would make the breaker
    /// a bigger outage than the thing it is protecting against.
    pub threshold: u32,
    /// The first cooldown. Doubles per consecutive trip.
    pub cooldown: Duration,
    /// Longest a breaker will ever wait before probing again.
    ///
    /// A cap, because doubling forever means an endpoint that recovers after a long
    /// outage stays cut off for hours after it is healthy again.
    pub max_cooldown: Duration,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            threshold: 5,
            cooldown: Duration::from_secs(30),
            max_cooldown: Duration::from_secs(300),
        }
    }
}

impl Policy {
    /// How long to wait after the `trips`-th consecutive trip.
    ///
    /// `trips` is 1 for the first. Doubling is the same reasoning as the retry
    /// backoff: an endpoint that has failed its probe five times is unlikely to pass
    /// the sixth a minute later, and every probe against a dead server costs a
    /// worker a full request timeout.
    pub fn cooldown_for(&self, trips: u32) -> Duration {
        let doublings = trips.saturating_sub(1).min(32);
        let scaled = self
            .cooldown
            .saturating_mul(1u32.checked_shl(doublings).unwrap_or(u32::MAX));
        scaled.min(self.max_cooldown)
    }
}

/// The result of applying an event.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Transition {
    pub breaker: Breaker,
    /// Set when the breaker is now `Open`: how long until a probe may be issued.
    /// `None` in every other state.
    pub cooldown: Option<Duration>,
}

impl Transition {
    fn unchanged(breaker: Breaker) -> Self {
        Self {
            breaker,
            cooldown: None,
        }
    }

    /// Whether this transition changed anything, so a caller can skip a write.
    pub fn changed(&self, from: Breaker) -> bool {
        self.breaker != from || self.cooldown.is_some()
    }
}

/// Apply one event.
///
/// Total over every state and event pair, deliberately: a breaker that panics or
/// silently does nothing on an unexpected combination is worse than one that makes a
/// documented choice, because the unexpected combinations are exactly what a
/// distributed system produces.
pub fn transition(breaker: Breaker, event: Event, policy: &Policy) -> Transition {
    match (breaker.state, event) {
        // ---------------------------------------------------------------- Closed
        (State::Closed, Event::Attempted(Health::Alive)) => Transition::unchanged(Breaker {
            state: State::Closed,
            consecutive_failures: 0,
            // A working delivery clears the history. The next outage starts its
            // cooldown from the bottom rather than inheriting last week's.
            trips: 0,
        }),
        (State::Closed, Event::Attempted(Health::Failing)) => {
            let failures = breaker.consecutive_failures.saturating_add(1);
            if failures >= policy.threshold {
                open(breaker.trips.saturating_add(1), policy)
            } else {
                Transition::unchanged(Breaker {
                    consecutive_failures: failures,
                    ..breaker
                })
            }
        }
        // Nothing was learned, so nothing changes — and in particular the failure
        // streak is neither extended nor reset.
        (State::Closed, Event::Attempted(Health::Unknown)) => Transition::unchanged(breaker),
        // A stray expiry for a breaker that is not waiting on anything.
        (State::Closed, Event::CooldownExpired) => Transition::unchanged(breaker),

        // ------------------------------------------------------------------ Open
        (State::Open, Event::CooldownExpired) => Transition::unchanged(Breaker {
            state: State::HalfOpen,
            ..breaker
        }),
        // An attempt that was already in flight when the breaker opened. Its result
        // predates the decision to stop, so it is not evidence about now — and
        // letting a stale success close the breaker would undo a trip that five
        // other failures had just earned.
        (State::Open, Event::Attempted(_)) => Transition::unchanged(breaker),

        // -------------------------------------------------------------- HalfOpen
        // The probe worked. Back to normal, and the trip history is cleared so a
        // future outage starts from the shortest cooldown.
        (State::HalfOpen, Event::Attempted(Health::Alive)) => Transition::unchanged(Breaker {
            state: State::Closed,
            consecutive_failures: 0,
            trips: 0,
        }),
        // Still down. Open again, and wait longer this time.
        (State::HalfOpen, Event::Attempted(Health::Failing)) => {
            open(breaker.trips.saturating_add(1), policy)
        }
        // The probe never reached the network, so it answered nothing. Staying
        // half-open leaves the door open for another probe rather than starting a
        // fresh cooldown on no evidence.
        (State::HalfOpen, Event::Attempted(Health::Unknown)) => Transition::unchanged(breaker),
        // Already half-open; a second expiry adds nothing.
        (State::HalfOpen, Event::CooldownExpired) => Transition::unchanged(breaker),
    }
}

fn open(trips: u32, policy: &Policy) -> Transition {
    Transition {
        breaker: Breaker {
            state: State::Open,
            consecutive_failures: 0,
            trips,
        },
        cooldown: Some(policy.cooldown_for(trips)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_STATES: [State; 3] = [State::Closed, State::Open, State::HalfOpen];
    const ALL_EVENTS: [Event; 4] = [
        Event::Attempted(Health::Alive),
        Event::Attempted(Health::Failing),
        Event::Attempted(Health::Unknown),
        Event::CooldownExpired,
    ];

    fn policy() -> Policy {
        Policy {
            threshold: 3,
            cooldown: Duration::from_secs(10),
            max_cooldown: Duration::from_secs(80),
        }
    }

    fn at(state: State) -> Breaker {
        Breaker {
            state,
            consecutive_failures: 0,
            trips: 0,
        }
    }

    #[test]
    fn every_state_and_event_pair_is_total() {
        // The acceptance criterion, and the reason `transition` matches exhaustively
        // rather than falling through a catch-all. A distributed system produces
        // every combination eventually, including the ones that "cannot happen".
        for state in ALL_STATES {
            for event in ALL_EVENTS {
                let t = transition(at(state), event, &policy());
                // A cooldown is set if and only if the breaker ended up open.
                assert_eq!(
                    t.cooldown.is_some(),
                    t.breaker.state == State::Open && t.breaker != at(state),
                    "{state:?} + {event:?} produced {t:?}"
                );
            }
        }
    }

    #[test]
    fn consecutive_failures_trip_the_breaker_at_the_threshold() {
        let p = policy();
        let mut b = at(State::Closed);
        for i in 1..p.threshold {
            let t = transition(b, Event::Attempted(Health::Failing), &p);
            assert_eq!(t.breaker.state, State::Closed, "tripped early at {i}");
            assert_eq!(t.breaker.consecutive_failures, i);
            b = t.breaker;
        }
        let t = transition(b, Event::Attempted(Health::Failing), &p);
        assert_eq!(t.breaker.state, State::Open);
        assert_eq!(t.breaker.trips, 1);
        assert_eq!(t.cooldown, Some(Duration::from_secs(10)));
    }

    #[test]
    fn one_success_resets_the_streak() {
        // "Consecutive" is the whole point. An endpoint failing one request in five
        // is unhealthy in a way retries handle; it is not a dead server, and a
        // breaker that counted cumulative failures would eventually trip on every
        // endpoint that has ever failed.
        let p = policy();
        let mut b = at(State::Closed);
        for _ in 0..p.threshold - 1 {
            b = transition(b, Event::Attempted(Health::Failing), &p).breaker;
        }
        assert_eq!(b.consecutive_failures, p.threshold - 1);

        b = transition(b, Event::Attempted(Health::Alive), &p).breaker;
        assert_eq!(b.consecutive_failures, 0);
        assert_eq!(b.state, State::Closed);

        // And the count really did start over.
        b = transition(b, Event::Attempted(Health::Failing), &p).breaker;
        assert_eq!(b.state, State::Closed);
        assert_eq!(b.consecutive_failures, 1);
    }

    #[test]
    fn an_unknown_outcome_neither_advances_nor_resets_the_streak() {
        // A refused address or an unparseable URL never reached the network. Counting
        // it as a failure would trip the breaker on an endpoint that was never
        // contacted; counting it as a success would let a misconfigured endpoint
        // indefinitely postpone a trip.
        let p = policy();
        let b = Breaker {
            state: State::Closed,
            consecutive_failures: 2,
            trips: 0,
        };
        let t = transition(b, Event::Attempted(Health::Unknown), &p);
        assert_eq!(t.breaker, b);
    }

    #[test]
    fn the_cooldown_expiring_moves_open_to_half_open() {
        let t = transition(at(State::Open), Event::CooldownExpired, &policy());
        assert_eq!(t.breaker.state, State::HalfOpen);
        assert_eq!(t.cooldown, None);
    }

    #[test]
    fn a_successful_probe_closes_the_breaker_and_clears_its_history() {
        let p = policy();
        let b = Breaker {
            state: State::HalfOpen,
            consecutive_failures: 4,
            trips: 6,
        };
        let t = transition(b, Event::Attempted(Health::Alive), &p);
        assert_eq!(t.breaker.state, State::Closed);
        assert_eq!(t.breaker.consecutive_failures, 0);
        // Cleared, so the next outage starts at the shortest cooldown rather than
        // inheriting an hour-long one from last week.
        assert_eq!(t.breaker.trips, 0);
    }

    #[test]
    fn a_failed_probe_reopens_with_a_longer_cooldown() {
        let p = policy();
        let mut b = Breaker {
            state: State::HalfOpen,
            consecutive_failures: 0,
            trips: 1,
        };
        for (expected_trips, expected) in [(2, 20), (3, 40), (4, 80), (5, 80)] {
            let t = transition(b, Event::Attempted(Health::Failing), &p);
            assert_eq!(t.breaker.state, State::Open);
            assert_eq!(t.breaker.trips, expected_trips);
            assert_eq!(t.cooldown, Some(Duration::from_secs(expected)));
            // Straight back to half-open for the next round.
            b = Breaker {
                state: State::HalfOpen,
                ..t.breaker
            };
        }
    }

    #[test]
    fn the_cooldown_is_capped() {
        // Doubling forever means an endpoint that recovers after a long outage stays
        // cut off for hours after it is healthy again.
        let p = policy();
        assert_eq!(p.cooldown_for(1), Duration::from_secs(10));
        assert_eq!(p.cooldown_for(4), Duration::from_secs(80));
        assert_eq!(p.cooldown_for(50), p.max_cooldown);
        assert_eq!(p.cooldown_for(u32::MAX), p.max_cooldown);
        // And it never underflows on a zeroth trip.
        assert_eq!(p.cooldown_for(0), Duration::from_secs(10));
    }

    #[test]
    fn an_open_breaker_ignores_attempts_that_were_already_in_flight() {
        // A request that started before the breaker opened reports after it. Its
        // result predates the decision to stop, so letting a stale success close the
        // breaker would undo a trip that five other failures had just earned.
        let p = policy();
        let b = Breaker {
            state: State::Open,
            consecutive_failures: 0,
            trips: 3,
        };
        for h in [Health::Alive, Health::Failing, Health::Unknown] {
            assert_eq!(transition(b, Event::Attempted(h), &p).breaker, b, "{h:?}");
        }
    }

    #[test]
    fn a_probe_that_never_left_the_process_leaves_the_breaker_half_open() {
        let p = policy();
        let b = at(State::HalfOpen);
        let t = transition(b, Event::Attempted(Health::Unknown), &p);
        // Not reopened: starting a fresh, longer cooldown on no evidence would keep
        // a recovered endpoint cut off because of a bad URL on one delivery.
        assert_eq!(t.breaker, b);
    }

    #[test]
    fn only_a_missing_answer_or_a_server_fault_counts_against_an_endpoint() {
        // The issue's note, as a test. A stream of 404s is a misconfigured URL and a
        // 429 is a working server asking us to slow down. Both servers are up, and
        // opening the breaker on either hides a problem that needs a person while
        // cutting off a destination that was fine.
        for status in [200, 201, 204, 301, 400, 401, 403, 404, 410, 422, 429] {
            assert_eq!(health(Some(status), None), Health::Alive, "{status}");
        }
        for status in [500, 502, 503, 504, 599] {
            assert_eq!(health(Some(status), None), Health::Failing, "{status}");
        }
        for t in [Transport::Timeout, Transport::Connect, Transport::Other] {
            assert_eq!(health(None, Some(t)), Health::Failing, "{t:?}");
        }
        assert_eq!(health(None, Some(Transport::Invalid)), Health::Unknown);
        assert_eq!(health(None, None), Health::Unknown);
    }

    #[test]
    fn a_status_wins_over_a_transport_error() {
        // If a status came back the server answered, whatever else went wrong
        // afterwards — a body that failed to read does not make the server dead.
        assert_eq!(
            health(Some(200), Some(Transport::Other)),
            Health::Alive,
            "a delivered response is proof of life"
        );
    }

    #[test]
    fn the_default_policy_does_not_trip_on_noise() {
        // One timeout is a dropped packet or a restarting process. Cutting off an
        // endpoint on that evidence would make the breaker a bigger outage than the
        // thing it protects against.
        let p = Policy::default();
        assert_eq!(p.threshold, 5);
        let mut b = at(State::Closed);
        for _ in 0..4 {
            b = transition(b, Event::Attempted(Health::Failing), &p).breaker;
            assert_eq!(b.state, State::Closed);
        }
        assert_eq!(
            transition(b, Event::Attempted(Health::Failing), &p)
                .breaker
                .state,
            State::Open
        );
    }

    #[test]
    fn a_state_round_trips_through_its_stored_form() {
        // The state lives in Postgres, so the strings are an interface.
        for s in ALL_STATES {
            assert_eq!(State::parse(s.as_str()), Some(s));
        }
        assert_eq!(State::parse("halfopen"), None);
        assert_eq!(State::parse(""), None);
    }
}
