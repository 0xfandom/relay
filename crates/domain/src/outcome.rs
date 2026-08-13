//! Deciding what an attempt amounted to.
//!
//! One function, one rule. Every retry decision in the system comes through here,
//! because a retry rule that lives in several places drifts: one caller learns that
//! `429` is worth retrying, another does not, and the difference only shows up as a
//! customer wondering why some of their webhooks arrive and some do not.
//!
//! Getting this wrong is expensive in both directions. Retrying a `404` for hours
//! spends the delivery budget on something that was never going to work, and from
//! the endpoint's side a stream of repeated requests to a URL that does not exist is
//! indistinguishable from an attack. Giving up on a `503` throws away a delivery
//! because a server was briefly restarting.

/// What one attempt amounted to.
///
/// Three variants, not two. "Failed" is not actionable on its own — the only
/// question that matters afterwards is whether trying again could plausibly produce
/// a different answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// The endpoint accepted it. Done.
    Success,
    /// Might work later. Worth another attempt.
    Retryable,
    /// Will not work later. Retrying only wastes attempts.
    Permanent,
}

impl Class {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
        }
    }

    pub fn is_retryable(self) -> bool {
        matches!(self, Self::Retryable)
    }
}

/// Why a request never produced a response.
///
/// Deliberately not `reqwest::Error`. This crate has no HTTP client and must not
/// gain one — the caller translates whatever its client reports into these, which
/// keeps the rule testable without a network and swappable without a rewrite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// No answer inside the timeout.
    Timeout,
    /// Could not reach the host: refused, unreachable, DNS, TLS.
    Connect,
    /// The request itself is unusable — malformed URL, unsupported scheme. No
    /// amount of retrying will make it valid.
    Invalid,
    /// Anything else the client reported.
    Other,
}

/// Classify a response status.
///
/// Total by construction: every `u16` maps to exactly one variant.
pub fn classify_status(status: u16) -> Class {
    match status {
        200..=299 => Class::Success,

        // Explicitly temporary, and the endpoint is telling us so. `429` and `503`
        // usually carry `Retry-After`, which #11 honours.
        408 | 425 | 429 => Class::Retryable,

        // The server broke, not the request. Restarts, deploys, overload, a
        // dependency of theirs being down — all of it recovers on its own.
        500..=599 => Class::Retryable,

        // Redirects are refused rather than followed, so a `3xx` here means the
        // endpoint has moved and its owner needs to update the URL. Retrying the
        // old one forever will not do that for them. Following it would be worse:
        // a redirect is one of the ways a URL that passed validation ends up
        // pointing somewhere internal.
        300..=399 => Class::Permanent,

        // The remaining `4xx` are all "your request is wrong": the URL does not
        // exist, the credentials are bad, the payload is rejected. None of that
        // changes by asking again.
        400..=499 => Class::Permanent,

        // `1xx` is not a final response, and anything outside `100..=599` is not a
        // status code at all. Either means the peer is not speaking HTTP properly,
        // which retrying does not fix.
        _ => Class::Permanent,
    }
}

/// Classify a failure to get any response at all.
///
/// Transport failures are retryable by default. The endpoint was unreachable rather
/// than unwilling, and unreachable is usually temporary — a restart, a network
/// blip, a full connection table.
pub fn classify_transport(transport: Transport) -> Class {
    match transport {
        Transport::Timeout | Transport::Connect | Transport::Other => Class::Retryable,
        // The exception. A URL that cannot be parsed will not parse on the fourth
        // attempt either.
        Transport::Invalid => Class::Permanent,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn success_is_the_whole_2xx_range() {
        for status in 200..=299 {
            assert_eq!(
                classify_status(status),
                Class::Success,
                "{status} should be success"
            );
        }
    }

    #[test]
    fn server_errors_are_retryable() {
        for status in 500..=599 {
            assert_eq!(
                classify_status(status),
                Class::Retryable,
                "{status} should be retryable"
            );
        }
    }

    #[test]
    fn the_temporary_four_hundreds_are_retryable() {
        // The endpoint is explicitly saying "not now", which is different from the
        // rest of the 4xx range saying "not ever".
        for status in [408, 425, 429] {
            assert_eq!(
                classify_status(status),
                Class::Retryable,
                "{status} should be retryable"
            );
        }
    }

    #[test]
    fn other_client_errors_are_permanent() {
        for status in [400, 401, 403, 404, 405, 410, 413, 418, 422, 451, 499] {
            assert_eq!(
                classify_status(status),
                Class::Permanent,
                "{status} should be permanent"
            );
        }
    }

    #[test]
    fn redirects_are_permanent() {
        // Redirects are not followed, so a 3xx is a moved endpoint whose owner has
        // to update the URL. Retrying cannot do that for them.
        for status in 300..=399 {
            assert_eq!(
                classify_status(status),
                Class::Permanent,
                "{status} should be permanent"
            );
        }
    }

    #[test]
    fn transport_failures_are_retryable_except_invalid_requests() {
        assert_eq!(classify_transport(Transport::Timeout), Class::Retryable);
        assert_eq!(classify_transport(Transport::Connect), Class::Retryable);
        assert_eq!(classify_transport(Transport::Other), Class::Retryable);
        assert_eq!(classify_transport(Transport::Invalid), Class::Permanent);
    }

    #[test]
    fn classification_is_total_and_never_panics() {
        // Every `u16`, not a sample. The acceptance criterion is that classification
        // is total, and the cheapest way to be sure is to check all 65,536 of them —
        // a gap in the match would be a panic here rather than a surprise in
        // production on some status nobody thought of.
        for status in u16::MIN..=u16::MAX {
            let class = classify_status(status);
            assert!(
                matches!(class, Class::Success | Class::Retryable | Class::Permanent),
                "{status} produced no class"
            );
        }
    }

    #[test]
    fn only_two_hundreds_ever_succeed() {
        // Guards the boundaries in both directions: nothing outside 2xx may report
        // success, and every 2xx must. An off-by-one at 199 or 300 would mean either
        // silently dropping deliveries or reporting failures as delivered.
        for status in u16::MIN..=u16::MAX {
            let is_success = classify_status(status) == Class::Success;
            assert_eq!(
                is_success,
                (200..=299).contains(&status),
                "{status} classified as success={is_success}"
            );
        }
    }

    #[test]
    fn is_retryable_agrees_with_the_class() {
        assert!(classify_status(503).is_retryable());
        assert!(!classify_status(404).is_retryable());
        assert!(!classify_status(200).is_retryable());
    }
}
