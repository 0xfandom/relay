//! Rules for the `Idempotency-Key` header.
//!
//! A producer that sends us an event and never sees the reply cannot tell whether
//! we received it. Both answers to that are wrong: not retrying loses the webhook,
//! retrying delivers it twice. The way out is for the producer to name its request,
//! so a retry is recognisable as the same request rather than a new one.
//!
//! What lives here is the part with no I/O in it: what counts as a usable key, and
//! how a request is fingerprinted so that reusing a key for a *different* request
//! is caught instead of silently swallowing the second one. The storage and the
//! race live in `relay-store`.

use std::time::Duration;

use sha2::{Digest, Sha256};

/// Longest key we will accept.
///
/// Keys are opaque to us — a UUID, a database primary key, a hash of the payload —
/// so the only thing worth bounding is size. 255 is generous for every one of those
/// and small enough that a caller cannot use the key column as free storage.
pub const MAX_KEY_LEN: usize = 255;

/// How long a key is honoured after its first use.
///
/// A window, not forever. Keys must be retained long enough to cover every retry a
/// sane producer will make — that is minutes, not days — and 24 hours leaves room
/// for a producer that queues failed requests and drains the queue the next
/// morning. Keeping them permanently would mean the table grows as fast as the
/// event table and never shrinks, to answer a question nobody asks after the first
/// hour.
///
/// The cost of the window is real and has to be stated: a duplicate arriving after
/// it expires creates a second event. That is the trade, and it is why the window
/// is documented rather than merely configured.
pub const RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

/// Why a key was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BadKey {
    /// Present but empty. Almost always a caller interpolating a variable that was
    /// never set, which would otherwise make every one of their requests collide.
    Empty,
    TooLong(usize),
    /// Contains control characters or non-ASCII bytes.
    Unprintable,
}

impl std::error::Error for BadKey {}

impl std::fmt::Display for BadKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "idempotency key is empty"),
            Self::TooLong(n) => {
                write!(
                    f,
                    "idempotency key is {n} bytes, the maximum is {MAX_KEY_LEN}"
                )
            }
            Self::Unprintable => write!(
                f,
                "idempotency key must be printable ASCII with no control characters"
            ),
        }
    }
}

/// Whether a key is usable.
///
/// Restrictive on purpose. A key is a database primary key and appears in logs, so
/// permitting control characters buys nothing and invites log injection and
/// unreadable diagnostics.
pub fn check_key(key: &str) -> Result<(), BadKey> {
    if key.is_empty() {
        return Err(BadKey::Empty);
    }
    if key.len() > MAX_KEY_LEN {
        return Err(BadKey::TooLong(key.len()));
    }
    if !key.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return Err(BadKey::Unprintable);
    }
    Ok(())
}

/// Domain separator. Without one, this hash could be confused with any other
/// SHA-256 in the system, and a value computed for one purpose could be replayed
/// as evidence for another.
const DOMAIN: &[u8] = b"relay/idempotency/v1";

/// A fingerprint of the request a key was first used for.
///
/// The point is not secrecy — nothing here is secret — it is detecting a caller
/// that reuses one key for two different requests. Without this, the second request
/// would be answered with the first one's result and silently dropped, which is a
/// lost event that looks like a success. With it, the caller gets an error naming
/// their own bug.
///
/// The length prefix is what makes the encoding injective. Concatenating
/// `event_type` and the body directly would give `("ab", "c")` and `("a", "bc")`
/// the same fingerprint, so two genuinely different requests could share one.
pub fn digest(event_type: &str, body: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(DOMAIN);
    h.update((event_type.len() as u64).to_be_bytes());
    h.update(event_type.as_bytes());
    h.update((body.len() as u64).to_be_bytes());
    h.update(body);
    h.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_keys_are_accepted() {
        for k in [
            "550e8400-e29b-41d4-a716-446655440000",
            "order-123",
            "1",
            "a/b:c_d.e~f",
            &"x".repeat(MAX_KEY_LEN),
        ] {
            assert_eq!(check_key(k), Ok(()), "{k} should be accepted");
        }
    }

    #[test]
    fn an_empty_key_is_refused() {
        // `Idempotency-Key: ` is what an unset template variable looks like. Taking
        // it at face value would make every request from that caller collide with
        // every other, so the whole stream would deduplicate down to one event.
        assert_eq!(check_key(""), Err(BadKey::Empty));
    }

    #[test]
    fn an_oversized_key_is_refused() {
        let k = "x".repeat(MAX_KEY_LEN + 1);
        assert_eq!(check_key(&k), Err(BadKey::TooLong(MAX_KEY_LEN + 1)));
    }

    #[test]
    fn unprintable_keys_are_refused() {
        for k in [
            "has space",
            "has\ttab",
            "has\nnewline",
            "has\0nul",
            "café",
            "\u{7f}",
        ] {
            assert_eq!(check_key(k), Err(BadKey::Unprintable), "{k:?}");
        }
    }

    #[test]
    fn the_same_request_fingerprints_the_same_way() {
        assert_eq!(
            digest("order.paid", br#"{"a":1}"#),
            digest("order.paid", br#"{"a":1}"#)
        );
    }

    #[test]
    fn a_different_body_fingerprints_differently() {
        assert_ne!(
            digest("order.paid", br#"{"a":1}"#),
            digest("order.paid", br#"{"a":2}"#)
        );
    }

    #[test]
    fn a_different_event_type_fingerprints_differently() {
        // Same bytes, different meaning. Fanning out to a different set of endpoints
        // is a different request even when the payload is identical.
        assert_ne!(
            digest("order.paid", br#"{"a":1}"#),
            digest("order.refunded", br#"{"a":1}"#)
        );
    }

    #[test]
    fn the_boundary_between_type_and_body_cannot_be_moved() {
        // The reason for the length prefixes. Without them these two collide, and a
        // caller could be handed the wrong event's result.
        assert_ne!(digest("ab", b"c"), digest("a", b"bc"));
    }

    #[test]
    fn an_empty_body_is_still_distinguishable() {
        assert_ne!(digest("a", b""), digest("", b"a"));
    }
}
