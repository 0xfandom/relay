//! When to try again.
//!
//! Two things are happening here and it is worth separating them.
//!
//! **Exponential** growth protects the endpoint. A server that failed once is often
//! struggling, and retrying at a fixed interval keeps the pressure on exactly when
//! it can least take it. Doubling backs off as fast as the problem is likely to
//! last.
//!
//! **Jitter** protects it from us. An endpoint goes down for an hour and ten
//! thousand deliveries pile up. If every one of them backs off by the same amount,
//! every one retries at the same instant, and the moment the endpoint comes back it
//! is hit by the entire backlog simultaneously — a self-inflicted flood, timed
//! precisely for when it is weakest. It falls over, everything fails again, and the
//! whole backlog re-synchronises for the next round.
//!
//! Randomising the delay smears the backlog across the window instead. It is the
//! difference between a recovering endpoint receiving a wave and receiving a trickle.
//!
//! Nothing here reads a clock or a random number generator. `next_delay` takes the
//! randomness as an argument, which keeps the crate's no-I/O rule intact and makes
//! the schedule exhaustively testable — the same inputs always produce the same
//! delay.

use std::time::Duration;

/// A delay is never zero: a retry that fires immediately is not a retry, it is the
/// same request again, and it would burn the attempt budget in milliseconds.
const MIN_DELAY: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    /// The first delay's ceiling. Doubles from here.
    pub base: Duration,
    /// The ceiling stops growing here. Without it, attempt 20 would schedule a
    /// delivery twelve days out.
    pub cap: Duration,
    /// How many attempts a delivery gets before it is given up on.
    pub max_attempts: u32,
    /// Longest `Retry-After` worth honouring. An endpoint asking for a day is
    /// either broken or hostile, and either way the answer is no.
    pub retry_after_cap: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            cap: Duration::from_secs(3600),
            // Roughly a day of coverage once the cap is reached, which outlasts most
            // outages without holding a delivery forever.
            max_attempts: 12,
            retry_after_cap: Duration::from_secs(300),
        }
    }
}

impl Backoff {
    /// The longest this attempt may be delayed: `base * 2^attempt`, capped.
    ///
    /// Deterministic and monotonic — this is the envelope the jitter fills. Kept
    /// separate from [`Backoff::next_delay`] because "does the schedule grow and
    /// stay bounded" and "is the delay spread out" are different questions and a
    /// random value cannot answer the first one.
    pub fn ceiling(&self, attempt: u32) -> Duration {
        // Saturating throughout: attempt is unbounded in principle, and an overflow
        // here would wrap to a tiny delay — a retry storm produced by the very code
        // meant to prevent one.
        let factor = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
        let grown = self
            .base
            .as_millis()
            .saturating_mul(u128::from(factor))
            .min(u128::from(u64::MAX));
        Duration::from_millis(grown as u64).min(self.cap)
    }

    /// How long to wait before attempt `attempt`.
    ///
    /// `jitter` is a fraction in `[0, 1]` from the caller's random source. Full
    /// jitter — the delay is spread over the whole window rather than a band near
    /// the top — because spreading the backlog matters more than any individual
    /// delivery waiting a particular length of time.
    ///
    /// Values outside `[0, 1]`, including NaN, are treated as `1.0`. A broken random
    /// source should back off the most, not the least.
    pub fn next_delay(&self, attempt: u32, jitter: f64) -> Duration {
        let jitter = if jitter.is_finite() {
            jitter.clamp(0.0, 1.0)
        } else {
            1.0
        };

        let ceiling = self.ceiling(attempt);
        let delayed = (ceiling.as_millis() as f64 * jitter) as u64;
        Duration::from_millis(delayed).max(MIN_DELAY)
    }

    /// Whether a delivery that has just finished attempt `attempt` has any left.
    pub fn attempts_remain(&self, attempt: u32) -> bool {
        attempt.saturating_add(1) < self.max_attempts
    }

    /// Honour an endpoint's own `Retry-After`, clamped.
    ///
    /// When a server says how long to wait, it knows more than our schedule does —
    /// a rate limiter usually knows exactly when the window resets. Clamped anyway,
    /// so a header of `86400` cannot park a delivery for a day.
    pub fn retry_after(&self, requested: Duration) -> Duration {
        requested.min(self.retry_after_cap).max(MIN_DELAY)
    }
}

/// Parse the delta-seconds form of `Retry-After`.
///
/// The header also permits an HTTP-date, which is deliberately not handled here:
/// resolving a date into a delay requires reading the clock, and this crate does
/// not. An unparseable value returns `None` and the caller falls back to the normal
/// schedule, which is always a safe answer.
pub fn parse_retry_after(value: &str) -> Option<Duration> {
    let secs: u64 = value.trim().parse().ok()?;
    Some(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backoff() -> Backoff {
        Backoff {
            base: Duration::from_secs(1),
            cap: Duration::from_secs(60),
            max_attempts: 5,
            retry_after_cap: Duration::from_secs(300),
        }
    }

    #[test]
    fn the_ceiling_doubles_until_it_reaches_the_cap() {
        let b = backoff();
        assert_eq!(b.ceiling(0), Duration::from_secs(1));
        assert_eq!(b.ceiling(1), Duration::from_secs(2));
        assert_eq!(b.ceiling(2), Duration::from_secs(4));
        assert_eq!(b.ceiling(3), Duration::from_secs(8));
        assert_eq!(b.ceiling(4), Duration::from_secs(16));
        assert_eq!(b.ceiling(5), Duration::from_secs(32));
        // 64s would exceed the cap.
        assert_eq!(b.ceiling(6), Duration::from_secs(60));
        assert_eq!(b.ceiling(7), Duration::from_secs(60));
    }

    #[test]
    fn the_ceiling_is_monotonic_and_never_exceeds_the_cap() {
        let b = backoff();
        let mut previous = Duration::ZERO;
        for attempt in 0..1_000 {
            let ceiling = b.ceiling(attempt);
            assert!(
                ceiling >= previous,
                "ceiling went backwards at attempt {attempt}"
            );
            assert!(
                ceiling <= b.cap,
                "ceiling exceeded the cap at attempt {attempt}"
            );
            previous = ceiling;
        }
    }

    #[test]
    fn a_huge_attempt_number_does_not_overflow_into_a_tiny_delay() {
        // The failure this guards against is specific: `1 << 64` wraps, the ceiling
        // collapses to something small, and the code meant to prevent a retry storm
        // produces one.
        let b = backoff();
        for attempt in [31, 32, 63, 64, 65, 1000, u32::MAX] {
            assert_eq!(
                b.ceiling(attempt),
                b.cap,
                "attempt {attempt} did not saturate at the cap"
            );
        }
    }

    #[test]
    fn a_delay_is_never_zero_whatever_the_jitter() {
        let b = backoff();
        // Sweeping the jitter space rather than sampling it: zero delay is only
        // reachable at particular values, and a random test would find it rarely.
        for attempt in 0..40 {
            for step in 0..=1000 {
                let jitter = step as f64 / 1000.0;
                let delay = b.next_delay(attempt, jitter);
                assert!(
                    delay >= MIN_DELAY,
                    "attempt {attempt} with jitter {jitter} produced {delay:?}"
                );
            }
        }
    }

    #[test]
    fn a_delay_never_exceeds_its_ceiling() {
        let b = backoff();
        for attempt in 0..40 {
            for step in 0..=1000 {
                let jitter = step as f64 / 1000.0;
                let delay = b.next_delay(attempt, jitter);
                assert!(
                    delay <= b.ceiling(attempt).max(MIN_DELAY),
                    "attempt {attempt} with jitter {jitter} exceeded its ceiling"
                );
            }
        }
    }

    #[test]
    fn a_broken_random_source_backs_off_the_most() {
        let b = backoff();
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0, 2.0, 1e18] {
            let delay = b.next_delay(3, bad);
            assert!(
                delay <= b.ceiling(3),
                "jitter {bad} produced {delay:?}, beyond the ceiling"
            );
        }
        // NaN and infinity mean full delay, not no delay: a broken source must fail
        // towards backing off.
        assert_eq!(b.next_delay(3, f64::NAN), b.ceiling(3));
        assert_eq!(b.next_delay(3, f64::INFINITY), b.ceiling(3));
    }

    #[test]
    fn a_backlog_is_spread_across_the_window_rather_than_synchronised() {
        let b = backoff();
        let attempt = 5; // ceiling 32s

        // Ten thousand deliveries that all failed at the same instant, each with its
        // own jitter value. Without jitter every one of these would be identical and
        // they would all retry together.
        let delays: Vec<Duration> = (0..10_000)
            .map(|i| b.next_delay(attempt, i as f64 / 10_000.0))
            .collect();

        let ceiling = b.ceiling(attempt);
        let buckets = 8;
        let bucket_width = ceiling.as_millis() / buckets;
        let mut counts = vec![0usize; buckets as usize];
        for d in &delays {
            let bucket = ((d.as_millis() / bucket_width) as usize).min(buckets as usize - 1);
            counts[bucket] += 1;
        }

        // Every part of the window is used. A schedule that clustered — jitter
        // applied to only the top half, say — would leave early buckets empty and
        // still deliver a wave, just a narrower one.
        for (i, count) in counts.iter().enumerate() {
            assert!(
                *count > 0,
                "no delivery scheduled in window bucket {i}: {counts:?}"
            );
        }

        let expected = delays.len() / buckets as usize;
        for (i, count) in counts.iter().enumerate() {
            assert!(
                *count > expected / 2,
                "bucket {i} holds {count}, far below an even spread of {expected}: {counts:?}"
            );
        }
    }

    #[test]
    fn attempts_run_out_at_max_attempts() {
        let b = backoff(); // max_attempts 5
        assert!(b.attempts_remain(0));
        assert!(b.attempts_remain(3));
        // Attempt 4 is the fifth and last.
        assert!(!b.attempts_remain(4));
        assert!(!b.attempts_remain(5));
        assert!(!b.attempts_remain(u32::MAX));
    }

    #[test]
    fn retry_after_is_honoured_but_clamped() {
        let b = backoff();
        assert_eq!(
            b.retry_after(Duration::from_secs(30)),
            Duration::from_secs(30)
        );
        // A day, refused.
        assert_eq!(
            b.retry_after(Duration::from_secs(86_400)),
            b.retry_after_cap
        );
        // Zero, refused in the other direction.
        assert_eq!(b.retry_after(Duration::ZERO), MIN_DELAY);
    }

    #[test]
    fn retry_after_parses_delta_seconds_and_rejects_the_rest() {
        assert_eq!(parse_retry_after("120"), Some(Duration::from_secs(120)));
        assert_eq!(parse_retry_after("  7 "), Some(Duration::from_secs(7)));
        assert_eq!(parse_retry_after("0"), Some(Duration::ZERO));

        // The HTTP-date form is not handled: resolving it needs a clock. Returning
        // None falls back to the normal schedule, which is always safe.
        assert_eq!(parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"), None);
        assert_eq!(parse_retry_after("-5"), None);
        assert_eq!(parse_retry_after(""), None);
        assert_eq!(parse_retry_after("soon"), None);
    }
}
