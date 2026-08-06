//! HMAC-SHA256 request signing.
//!
//! Every webhook Relay sends carries proof that it came from us. The customer
//! holds a shared secret; we sign, they recompute, they compare.
//!
//! Wire format (deliberately Stripe-shaped, so their docs are a reference):
//!
//! ```text
//! Relay-Timestamp: 1700000000
//! Relay-Signature: v1=f592bbf3951cfc94...
//! ```
//!
//! The signed string is EXACTLY:  `<timestamp>.<raw body bytes>`
//!
//! Two design points you should be able to defend before you write the code:
//!
//! 1. Why is the timestamp inside the signed string, rather than just a header?
//! 2. Why does `body` have type `&[u8]` instead of `&str` or a parsed struct?
//!
//! (Both answers are in the PRD, §Signatures. The tests below will fail loudly
//! if you get #2 wrong.)

/// Build the byte string that gets signed: `<timestamp>.<body>`.
///
/// Returns bytes, not a `String`, because a webhook body is arbitrary bytes and
/// need not be valid UTF-8.
pub fn signed_payload(timestamp: i64, body: &[u8]) -> Vec<u8> {
    todo!("build `<timestamp>.<body>` as a Vec<u8>")
}

/// HMAC-SHA256 over [`signed_payload`], hex-encoded lowercase.
///
/// `secret` is bytes rather than `&str` because HMAC keys are byte strings and
/// accept any length.
pub fn sign(secret: &[u8], timestamp: i64, body: &[u8]) -> String {
    todo!("HMAC-SHA256 the signed payload with `secret`, return lowercase hex")
}

/// Recompute the signature and compare it to `candidate_hex`.
///
/// The comparison MUST be constant-time. Using `==` on the two strings leaks
/// how many leading bytes were correct, via how long the comparison took, which
/// lets an attacker recover a valid signature one byte at a time.
///
/// This is the receiver's job in production — we implement it so `testkit` can
/// verify us, and so the property is tested from both sides.
pub fn verify(secret: &[u8], timestamp: i64, body: &[u8], candidate_hex: &str) -> bool {
    todo!("recompute and compare in constant time")
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real vectors, generated with:
    //   printf '1700000000.{"hello":"world"}' \
    //     | openssl dgst -sha256 -hmac "whsec_test" -r
    const SECRET: &[u8] = b"whsec_test";
    const TS: i64 = 1_700_000_000;
    const BODY: &[u8] = br#"{"hello":"world"}"#;
    const EXPECTED: &str = "f592bbf3951cfc94e560eecfb5d9dd4da6b0fff2e626235f8ab4b54860925d0b";

    #[test]
    fn signed_payload_is_timestamp_dot_body() {
        assert_eq!(
            signed_payload(TS, BODY),
            br#"1700000000.{"hello":"world"}"#.to_vec()
        );
    }

    #[test]
    fn matches_known_vector() {
        assert_eq!(sign(SECRET, TS, BODY), EXPECTED);
    }

    /// The whole reason `body` is `&[u8]`.
    ///
    /// These two bodies are the SAME JSON value and DIFFERENT bytes. If any
    /// layer of Relay ever parses the payload and re-serialises it, key order
    /// can change and every signature silently breaks.
    #[test]
    fn same_json_different_byte_order_is_a_different_signature() {
        let reordered = br#"{"world":"hello"}"#;
        assert_eq!(
            sign(SECRET, TS, reordered),
            "f536c2914ebc4843072f5c757f80acd1f38c9751fa29a07e9f753a14c5b6b1f5"
        );
        assert_ne!(sign(SECRET, TS, BODY), sign(SECRET, TS, reordered));
    }

    #[test]
    fn timestamp_changes_the_signature() {
        assert_ne!(sign(SECRET, TS, BODY), sign(SECRET, TS + 1, BODY));
    }

    #[test]
    fn verify_accepts_our_own_signature() {
        assert!(verify(SECRET, TS, BODY, &sign(SECRET, TS, BODY)));
    }

    #[test]
    fn verify_rejects_a_tampered_body() {
        let sig = sign(SECRET, TS, BODY);
        assert!(!verify(SECRET, TS, br#"{"hello":"evil"}"#, &sig));
    }

    #[test]
    fn verify_rejects_the_wrong_secret() {
        let sig = sign(SECRET, TS, BODY);
        assert!(!verify(b"whsec_other", TS, BODY, &sig));
    }

    #[test]
    fn verify_rejects_garbage_that_is_not_even_hex() {
        assert!(!verify(SECRET, TS, BODY, "not-hex-at-all"));
    }
}
