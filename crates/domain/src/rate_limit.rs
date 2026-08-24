//! How fast Relay may send to one endpoint.
//!
//! Retries decide *when to try again after a failure*. This decides *whether to send
//! at all*, before anything fails — because the failure a rate limit prevents is one
//! Relay causes. A customer subscribes to a high-volume event, we fan out a burst of
//! ten thousand deliveries, and their server falls over. Every one of those then
//! fails, retries, and arrives again in a wave.
//!
//! ## Why a bucket rather than a window
//!
//! Counting requests per fixed second is the obvious implementation and it is wrong
//! at the boundary: ten requests at `t=0.99` and ten more at `t=1.01` are two legal
//! seconds and twenty requests in twenty milliseconds. The endpoint experiences
//! double the configured rate at exactly the moment a burst is most likely.
//!
//! A bucket has no boundaries. Tokens accumulate continuously and are capped at
//! `burst`, so the most that can ever leave at once is `burst` — and the long-run
//! average cannot exceed `per_second` no matter how the traffic is shaped.
//!
//! ## No clock in here
//!
//! Like [`crate::backoff`], this module reads no clock. The caller measures elapsed
//! time and passes it in, which is what lets a test express "now imagine four
//! seconds pass" without waiting four seconds, and what keeps the whole policy
//! exhaustively testable.

use std::time::Duration;

/// How fast one endpoint may be sent to.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rate {
    /// Sustained deliveries per second.
    pub per_second: f64,
    /// The most that may go out at once after an idle period.
    ///
    /// Separate from the rate because they answer different questions. `per_second`
    /// is what the endpoint can sustain; `burst` is what it can absorb in one go.
    /// A burst of one would smooth traffic perfectly and make Relay incapable of
    /// ever catching up on a backlog.
    pub burst: f64,
}

impl Default for Rate {
    /// Deliberately conservative. Relay cannot know what a customer's server can
    /// take, and the cost of guessing low is a slower drain, while the cost of
    /// guessing high is their outage.
    fn default() -> Self {
        Self {
            per_second: 10.0,
            burst: 20.0,
        }
    }
}

/// The outcome of asking a bucket for one token.
///
/// Both arms carry the level the bucket is left at, so the caller stores one value
/// and never has to recompute the refill.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Take {
    /// A token was available and has been spent.
    Taken { tokens: f64 },
    /// None available. One arrives in `after`.
    Wait { tokens: f64, after: Duration },
}

impl Take {
    pub fn tokens(self) -> f64 {
        match self {
            Self::Taken { tokens } | Self::Wait { tokens, .. } => tokens,
        }
    }

    pub fn is_taken(self) -> bool {
        matches!(self, Self::Taken { .. })
    }
}

/// Slowest rate treated as a rate rather than a stop. Guards against a division by
/// zero producing an infinite wait, which would park a delivery forever.
const MIN_PER_SECOND: f64 = 0.001;

impl Rate {
    pub fn new(per_second: f64, burst: f64) -> Self {
        Self { per_second, burst }
    }

    /// The configured rate, with unusable values replaced.
    ///
    /// A burst below one can never hold a whole token, so the bucket would refuse
    /// every delivery forever — an endpoint silently switched off by a typo. The
    /// database rejects both cases too; this is the second lock on the same door,
    /// because rows written before the constraint existed do not re-validate.
    fn sane(self) -> Self {
        let per_second = if self.per_second.is_finite() && self.per_second > MIN_PER_SECOND {
            self.per_second
        } else {
            MIN_PER_SECOND
        };
        let burst = if self.burst.is_finite() && self.burst >= 1.0 {
            self.burst
        } else {
            1.0
        };
        Self { per_second, burst }
    }

    /// The level after `elapsed` of refilling, capped at `burst`.
    ///
    /// The cap is the entire safety property. Without it an endpoint idle overnight
    /// would accumulate 800,000 tokens and then receive every one of them at once,
    /// which is the flood the limiter exists to prevent, merely delayed.
    pub fn refill(&self, tokens: f64, elapsed: Duration) -> f64 {
        let rate = self.sane();
        let gained = elapsed.as_secs_f64() * rate.per_second;
        (tokens + gained).clamp(0.0, rate.burst)
    }

    /// How long until the bucket holds a whole token.
    pub fn wait_for_one(&self, tokens: f64) -> Duration {
        let rate = self.sane();
        if tokens >= 1.0 {
            return Duration::ZERO;
        }
        Duration::from_secs_f64((1.0 - tokens.max(0.0)) / rate.per_second)
    }

    /// Refill, then spend one token if there is one.
    pub fn take(&self, tokens: f64, elapsed: Duration) -> Take {
        let available = self.refill(tokens, elapsed);
        if available >= 1.0 {
            Take::Taken {
                tokens: available - 1.0,
            }
        } else {
            Take::Wait {
                tokens: available,
                after: self.wait_for_one(available),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(s: f64) -> Duration {
        Duration::from_secs_f64(s)
    }

    #[test]
    fn a_full_bucket_spends_one_token_per_delivery() {
        let rate = Rate::new(10.0, 5.0);
        let mut tokens = 5.0;
        for expected in [4.0, 3.0, 2.0, 1.0, 0.0] {
            let take = rate.take(tokens, Duration::ZERO);
            assert!(take.is_taken());
            assert_eq!(take.tokens(), expected);
            tokens = take.tokens();
        }
    }

    #[test]
    fn an_empty_bucket_refuses_and_says_when_to_come_back() {
        let rate = Rate::new(10.0, 5.0);
        match rate.take(0.0, Duration::ZERO) {
            Take::Wait { tokens, after } => {
                assert_eq!(tokens, 0.0);
                // Ten per second means one every hundred milliseconds.
                assert_eq!(after, secs(0.1));
            }
            other => panic!("expected a wait, got {other:?}"),
        }
    }

    #[test]
    fn waiting_exactly_long_enough_yields_exactly_one_token() {
        // The boundary that decides whether a deferred delivery comes back to a
        // token or to another deferral. Landing a hair short would make a busy
        // endpoint's deliveries bounce twice for every one that goes out.
        let rate = Rate::new(10.0, 5.0);
        let Take::Wait { after, .. } = rate.take(0.0, Duration::ZERO) else {
            panic!("expected a wait");
        };
        assert!(rate.take(0.0, after).is_taken());
    }

    #[test]
    fn tokens_refill_at_the_configured_rate() {
        let rate = Rate::new(10.0, 100.0);
        assert_eq!(rate.refill(0.0, secs(1.0)), 10.0);
        assert_eq!(rate.refill(0.0, secs(0.5)), 5.0);
        assert_eq!(rate.refill(2.0, secs(1.0)), 12.0);
    }

    #[test]
    fn an_idle_bucket_does_not_accumulate_unbounded_credit() {
        // The whole reason for the cap. Without it, an endpoint quiet overnight
        // would bank 864,000 tokens and receive all of them in one burst — exactly
        // the flood this exists to prevent, merely postponed.
        let rate = Rate::new(10.0, 20.0);
        assert_eq!(rate.refill(0.0, secs(86_400.0)), 20.0);
    }

    #[test]
    fn a_burst_is_capped_rather_than_doubled_at_a_boundary() {
        // The fixed-window bug, stated as a test. Ten per second counted in windows
        // permits ten at the end of one window and ten at the start of the next:
        // twenty in a moment. A bucket that has just been drained cannot do that,
        // however the traffic lines up with any clock.
        let rate = Rate::new(10.0, 10.0);
        let mut tokens = 10.0;
        for _ in 0..10 {
            tokens = rate.take(tokens, Duration::ZERO).tokens();
        }
        assert_eq!(tokens, 0.0);

        // A window boundary is not a thing the bucket has. Crossing one buys the
        // same tokens as any other equivalent interval.
        let refilled = rate.refill(tokens, secs(0.01));
        assert!(refilled < 1.0, "a boundary must not hand back a burst");
    }

    #[test]
    fn the_long_run_average_cannot_exceed_the_rate() {
        // Ten seconds of pressing as hard as possible against a 10/s bucket that
        // started full: 20 burst plus 100 earned. Anything more would mean the
        // limiter can be outrun by asking often enough.
        let rate = Rate::new(10.0, 20.0);
        let mut tokens = 20.0;
        let mut sent = 0;
        // A millisecond apart for ten seconds.
        for _ in 0..10_000 {
            let take = rate.take(tokens, secs(0.001));
            tokens = take.tokens();
            if take.is_taken() {
                sent += 1;
            }
        }
        assert!(
            (119..=121).contains(&sent),
            "sent {sent}, expected about 120"
        );
    }

    #[test]
    fn a_slow_rate_still_lets_traffic_through() {
        // One every ten seconds. Slow is not stopped.
        let rate = Rate::new(0.1, 1.0);
        assert!(rate.take(0.0, secs(10.0)).is_taken());
        assert_eq!(rate.wait_for_one(0.0), secs(10.0));
    }

    #[test]
    fn an_unusable_rate_never_parks_a_delivery_forever() {
        // A zero or negative rate would divide into an infinite wait, and a burst
        // below one could never hold a whole token. Either is an endpoint silently
        // switched off by a typo, so both are replaced rather than honoured.
        for bad in [0.0, -5.0, f64::NAN, f64::INFINITY] {
            let rate = Rate::new(bad, 1.0);
            let wait = rate.wait_for_one(0.0);
            assert!(wait.as_secs_f64().is_finite(), "{bad} gave {wait:?}");
            assert!(rate.take(0.0, secs(100_000.0)).is_taken(), "{bad}");
        }
        for bad in [0.0, 0.5, -1.0, f64::NAN] {
            let rate = Rate::new(10.0, bad);
            assert!(rate.take(0.0, secs(10.0)).is_taken(), "burst {bad}");
        }
    }

    #[test]
    fn the_default_is_conservative() {
        // Relay cannot know what a customer's server can take. Guessing low costs a
        // slower drain; guessing high costs them an outage.
        let d = Rate::default();
        assert_eq!(d.per_second, 10.0);
        assert_eq!(d.burst, 20.0);
    }
}
