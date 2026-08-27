//! What a delivery actually looks like on the wire.
//!
//! Until now every delivery was the same shape: `POST` the customer's exact bytes
//! with a signature header. A Telegram bot does not want that — it wants
//! `POST /bot<token>/sendMessage` with `{"chat_id": …, "text": …}` — and a Discord
//! webhook wants `{"content": …}` at a URL that carries its own credential.
//!
//! Everything *downstream* of building the request is identical: the same retries,
//! the same backoff, the same breaker, the same rate limit, the same attempt log.
//! So the only thing that varies is the request, and that is what this trait covers.
//! The test for whether the abstraction sits in the right place is blunt: if a
//! transport ever needs to change retry or breaker behaviour, it does not.
//!
//! Two rules that shape the whole design.
//!
//! **The credential never lives in the URL.** Telegram's bot token and Discord's
//! webhook token are both, in their native form, path segments — and a URL is
//! returned by the admin API, stored in dead letters, and written into a span on
//! every send. So an endpoint stores the *address* (a chat id, a webhook id) in its
//! `url` column and the *credential* in its `secret` column, which is already the
//! redacted type. The real URL is assembled here, used, and never written down.
//!
//! **Signing is a property of the transport, not of Relay.** The HMAC exists so a
//! receiver can prove a payload came from us. Telegram already knows the request
//! came from us, because it arrived with our bot token — a signature there would be
//! ceremony. So [`Outbound::signed`] is decided per transport rather than assumed.

use crate::signature;

/// The kinds of destination Relay can deliver to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A customer's own HTTP endpoint. The bytes that arrived, signed, verbatim.
    Http,
    Telegram,
    Discord,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Http => "http",
            Kind::Telegram => "telegram",
            Kind::Discord => "discord",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "http" => Some(Kind::Http),
            "telegram" => Some(Kind::Telegram),
            "discord" => Some(Kind::Discord),
            _ => None,
        }
    }
}

/// Telegram's Bot API.
pub const TELEGRAM_API: &str = "https://api.telegram.org";
/// Discord's API.
pub const DISCORD_API: &str = "https://discord.com/api";

/// The transports Relay knows about.
///
/// A registry rather than a `match` in the sender, so that adding a transport is a
/// change in this file and nowhere else — which is the acceptance criterion the
/// abstraction exists to meet.
///
/// The bases are configurable for one reason: without them the chat transports could
/// only ever be tested by pointing at the real Telegram, which means they would be
/// tested by hand once and never again. With them, the same code path a customer
/// gets is driven against a local stand-in on every run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registry {
    http: Http,
    telegram: Telegram,
    discord: Discord,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            http: Http,
            telegram: Telegram {
                base: TELEGRAM_API.to_string(),
            },
            discord: Discord {
                base: DISCORD_API.to_string(),
            },
        }
    }
}

impl Registry {
    /// A registry pointed at local stand-ins for the chat APIs.
    pub fn with_bases(telegram: impl Into<String>, discord: impl Into<String>) -> Self {
        Self {
            http: Http,
            telegram: Telegram {
                base: telegram.into(),
            },
            discord: Discord {
                base: discord.into(),
            },
        }
    }

    pub fn get(&self, kind: Kind) -> &dyn Transport {
        match kind {
            Kind::Http => &self.http,
            Kind::Telegram => &self.telegram,
            Kind::Discord => &self.discord,
        }
    }

    /// Build a request for `kind`.
    pub fn build(&self, kind: Kind, cx: &Context<'_>) -> Result<Outbound, BuildError> {
        self.get(kind).build(cx)
    }

    /// Whether an address is usable for `kind`.
    pub fn validate(&self, kind: Kind, address: &str) -> Result<(), BuildError> {
        self.get(kind).validate(address)
    }
}

/// Why a request could not be built.
///
/// Always permanent. Every variant describes something about the endpoint's own
/// configuration, and no amount of retrying makes a malformed chat id well formed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BuildError {
    #[error("address must be of the form {expected}, got {got:?}")]
    Address { expected: &'static str, got: String },
    #[error("{0}")]
    Empty(&'static str),
}

/// Everything the sender needs to make one request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outbound {
    /// The URL to connect to. May carry a credential, so it is never logged.
    pub url: String,
    /// The same URL with any credential replaced. This is the one that goes in a
    /// span, an error message and the attempt log.
    ///
    /// Two fields rather than a redacting function called at each log site, for the
    /// same reason `Secret` has no `Display`: a rule people have to remember fails
    /// the first time somebody adds a log line in a hurry.
    pub display_url: String,
    pub headers: Vec<(&'static str, String)>,
    pub body: Vec<u8>,
    /// Whether the request carries a Relay signature.
    pub signed: bool,
}

/// What a transport has to work with.
pub struct Context<'a> {
    /// The endpoint's stored address: a URL, a chat id, a webhook id.
    pub address: &'a str,
    /// The endpoint's secret: a signing key, a bot token, a webhook token.
    pub credential: &'a str,
    /// The secret being rotated away from, if a rotation window is open. Only the
    /// HTTP transport has any use for it — the others authenticate with a token in
    /// the URL, which cannot be sent twice.
    pub previous_credential: Option<&'a str>,
    pub event_type: &'a str,
    pub delivery_id: &'a str,
    /// The bytes that arrived. Never re-encoded by the HTTP transport; rendered as
    /// text by the chat ones, because a chat message is text.
    pub payload: &'a [u8],
    pub timestamp: i64,
}

pub trait Transport: Send + Sync {
    fn kind(&self) -> Kind;

    /// Build the request.
    fn build(&self, cx: &Context<'_>) -> Result<Outbound, BuildError>;

    /// Whether an address is usable, checked when an endpoint is registered.
    ///
    /// Separate from [`Transport::build`] so a caller can reject a bad address at the
    /// moment somebody can still fix it, rather than at 3am from a dead letter.
    fn validate(&self, address: &str) -> Result<(), BuildError>;
}

// ------------------------------------------------------------------------ http

/// The original transport: the customer's own URL, their exact bytes, signed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http;

impl Transport for Http {
    fn kind(&self) -> Kind {
        Kind::Http
    }

    fn build(&self, cx: &Context<'_>) -> Result<Outbound, BuildError> {
        // During a rotation's overlap window both secrets sign and both signatures go
        // out together, newest first. There is no ordering of "we switch" and "they
        // switch" that avoids failed deliveries otherwise.
        let mut signatures = format!(
            "v1={}",
            signature::sign(cx.credential.as_bytes(), cx.timestamp, cx.payload)
        );
        if let Some(previous) = cx.previous_credential {
            signatures.push(',');
            signatures.push_str(&format!(
                "v1={}",
                signature::sign(previous.as_bytes(), cx.timestamp, cx.payload)
            ));
        }

        Ok(Outbound {
            url: cx.address.to_string(),
            // Nothing secret in a customer's own URL, so there is nothing to hide.
            display_url: cx.address.to_string(),
            headers: vec![
                ("content-type", "application/json".to_string()),
                ("relay-timestamp", cx.timestamp.to_string()),
                ("relay-signature", signatures),
                // Stable across every attempt. If this changed per attempt, receivers
                // could not deduplicate retries.
                ("relay-delivery-id", cx.delivery_id.to_string()),
                ("relay-event-type", cx.event_type.to_string()),
            ],
            // Verbatim. Nothing may parse and re-encode this: JSON key order is not
            // defined, and the signature covers bytes rather than meaning.
            body: cx.payload.to_vec(),
            signed: true,
        })
    }

    fn validate(&self, address: &str) -> Result<(), BuildError> {
        // The real check is the URL policy in `url_guard`, applied by the caller to
        // the built destination. All this does is refuse an empty string.
        if address.trim().is_empty() {
            return Err(BuildError::Empty("url must not be empty"));
        }
        Ok(())
    }
}

// -------------------------------------------------------------------- telegram

/// Telegram's longest message. Anything past it is rejected by the API, so it is
/// truncated here instead — a message that arrives cut short is worth more than one
/// that does not arrive.
pub const TELEGRAM_MAX_TEXT: usize = 4096;

/// A Telegram chat, addressed as `telegram://<chat_id>`.
///
/// The bot token is the endpoint's secret, not part of the address, so it inherits
/// every protection the signing secret already has: redacted in `Debug`, never
/// serialised, never in a URL that gets logged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Telegram {
    /// Where the Bot API lives. The real one in production, a stand-in in tests.
    pub base: String,
}

impl Transport for Telegram {
    fn kind(&self) -> Kind {
        Kind::Telegram
    }

    fn build(&self, cx: &Context<'_>) -> Result<Outbound, BuildError> {
        let chat = chat_id(cx.address)?;
        Ok(Outbound {
            url: format!("{}/bot{}/sendMessage", self.base, cx.credential),
            display_url: format!("{}/bot<redacted>/sendMessage", self.base),
            headers: vec![("content-type", "application/json".to_string())],
            body: serde_json::to_vec(&serde_json::json!({
                "chat_id": chat,
                "text": render(cx.event_type, cx.payload, TELEGRAM_MAX_TEXT),
            }))
            .expect("a map of strings always serialises"),
            // Telegram already knows the request came from us: it arrived carrying our
            // bot token. A signature would be ceremony.
            signed: false,
        })
    }

    fn validate(&self, address: &str) -> Result<(), BuildError> {
        chat_id(address).map(|_| ())
    }
}

/// The chat id out of `telegram://<chat_id>`.
///
/// A scheme rather than a bare id so that the `url` column stays self-describing:
/// somebody reading the table can see what an endpoint is without joining to the
/// transport column.
fn chat_id(address: &str) -> Result<&str, BuildError> {
    let chat = address
        .strip_prefix("telegram://")
        .ok_or_else(|| BuildError::Address {
            expected: "telegram://<chat_id>",
            got: address.to_string(),
        })?;
    if chat.is_empty() || chat.contains('/') {
        return Err(BuildError::Address {
            expected: "telegram://<chat_id>",
            got: address.to_string(),
        });
    }
    Ok(chat)
}

// --------------------------------------------------------------------- discord

/// Discord's longest webhook message.
pub const DISCORD_MAX_CONTENT: usize = 2000;

/// A Discord webhook, addressed as `discord://<webhook_id>`.
///
/// Discord's own URL is `https://discord.com/api/webhooks/<id>/<token>`, and that
/// trailing token is a credential in a path segment — the exact shape this design
/// refuses to store in a `url` column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discord {
    pub base: String,
}

impl Transport for Discord {
    fn kind(&self) -> Kind {
        Kind::Discord
    }

    fn build(&self, cx: &Context<'_>) -> Result<Outbound, BuildError> {
        let id = webhook_id(cx.address)?;
        Ok(Outbound {
            url: format!("{}/webhooks/{id}/{}", self.base, cx.credential),
            // The id is not secret and is the only part worth having in a log: it is
            // what identifies which webhook failed.
            display_url: format!("{}/webhooks/{id}/<redacted>", self.base),
            headers: vec![("content-type", "application/json".to_string())],
            body: serde_json::to_vec(&serde_json::json!({
                "content": render(cx.event_type, cx.payload, DISCORD_MAX_CONTENT),
            }))
            .expect("a map of strings always serialises"),
            signed: false,
        })
    }

    fn validate(&self, address: &str) -> Result<(), BuildError> {
        webhook_id(address).map(|_| ())
    }
}

fn webhook_id(address: &str) -> Result<&str, BuildError> {
    let id = address
        .strip_prefix("discord://")
        .ok_or_else(|| BuildError::Address {
            expected: "discord://<webhook_id>",
            got: address.to_string(),
        })?;
    // Digits only. Discord ids are snowflakes, and refusing anything else stops a
    // path segment being smuggled into the URL this builds.
    if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
        return Err(BuildError::Address {
            expected: "discord://<webhook_id>",
            got: address.to_string(),
        });
    }
    Ok(id)
}

// ---------------------------------------------------------------------- shared

/// Turn a payload into something a human will read in a chat window.
///
/// A chat message is text, so the bytes have to become text — which is the one place
/// Relay's usual rule about never re-encoding a payload does not apply, because
/// nothing here is signed. The event type leads, because in a channel carrying four
/// kinds of event that is the first thing anyone needs.
fn render(event_type: &str, payload: &[u8], max: usize) -> String {
    let body = String::from_utf8_lossy(payload);
    truncate(&format!("{event_type}\n{body}"), max)
}

/// Truncate on a character boundary so the result is always valid UTF-8.
///
/// Truncating rather than refusing: a message that arrives cut short is worth more
/// than one that does not arrive, and the platform would reject the whole thing.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cx<'a>(address: &'a str, credential: &'a str, payload: &'a [u8]) -> Context<'a> {
        Context {
            address,
            credential,
            previous_credential: None,
            event_type: "order.paid",
            delivery_id: "11111111-2222-3333-4444-555555555555",
            payload,
            timestamp: 1_700_000_000,
        }
    }

    #[test]
    fn http_sends_the_payload_byte_for_byte() {
        // The whole signature design rests on this. One re-encoding anywhere — a
        // different key order, a re-indent — and every signature we send fails
        // verification at the receiver.
        let payload = br#"{"b":2,"a":1}"#;
        let out = Http
            .build(&cx("https://example.com/hook", "whsec_k", payload))
            .unwrap();
        assert_eq!(out.body, payload.to_vec());
        assert!(out.signed);
        assert_eq!(out.url, "https://example.com/hook");
        assert_eq!(out.display_url, out.url);
    }

    #[test]
    fn http_sends_both_signatures_during_a_rotation() {
        let payload = b"{}";
        let mut c = cx("https://example.com/hook", "new_secret", payload);
        c.previous_credential = Some("old_secret");
        let out = Http.build(&c).unwrap();

        let header = &out
            .headers
            .iter()
            .find(|(k, _)| *k == "relay-signature")
            .unwrap()
            .1;
        let parts: Vec<&str> = header.split(',').collect();
        assert_eq!(parts.len(), 2);
        // Newest first: a receiver that only checks the first entry — which the format
        // permits and the docs discourage — should be checking the one it is moving
        // to, not the one it is leaving.
        assert_eq!(
            parts[0],
            format!(
                "v1={}",
                signature::sign(b"new_secret", 1_700_000_000, payload)
            )
        );
        assert_eq!(
            parts[1],
            format!(
                "v1={}",
                signature::sign(b"old_secret", 1_700_000_000, payload)
            )
        );
    }

    #[test]
    fn a_chat_credential_never_appears_in_a_loggable_url() {
        let registry = Registry::default();
        let tg = registry
            .build(
                Kind::Telegram,
                &cx("telegram://-100123", "SECRET_TOKEN", b"{}"),
            )
            .unwrap();
        assert!(
            tg.url.contains("SECRET_TOKEN"),
            "it has to be in the real url"
        );
        assert!(!tg.display_url.contains("SECRET_TOKEN"));
        assert_eq!(
            tg.display_url,
            "https://api.telegram.org/bot<redacted>/sendMessage"
        );

        let dc = registry
            .build(
                Kind::Discord,
                &cx("discord://998877", "SECRET_TOKEN", b"{}"),
            )
            .unwrap();
        assert!(dc.url.contains("SECRET_TOKEN"));
        assert!(!dc.display_url.contains("SECRET_TOKEN"));
        // The webhook id survives, because it is not secret and it is the only part
        // worth having in a log: it says *which* webhook failed.
        assert_eq!(
            dc.display_url,
            "https://discord.com/api/webhooks/998877/<redacted>"
        );
    }

    #[test]
    fn chat_transports_do_not_sign() {
        let registry = Registry::default();
        for (kind, address) in [
            (Kind::Telegram, "telegram://-100123"),
            (Kind::Discord, "discord://998877"),
        ] {
            let out = registry.build(kind, &cx(address, "token", b"{}")).unwrap();
            assert!(!out.signed, "{kind:?} should not sign");
            assert!(
                !out.headers.iter().any(|(k, _)| k.starts_with("relay-")),
                "{kind:?} sent a Relay header"
            );
        }
    }

    #[test]
    fn the_event_type_leads_the_message() {
        let out = Registry::default()
            .build(
                Kind::Telegram,
                &cx("telegram://-100123", "t", br#"{"n":1}"#),
            )
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out.body).unwrap();
        // In a channel carrying four kinds of event, the type is the first thing
        // anybody needs to see.
        assert_eq!(v["text"], "order.paid\n{\"n\":1}");
        assert_eq!(v["chat_id"], "-100123");
    }

    #[test]
    fn a_message_past_the_platform_limit_is_cut_rather_than_dropped() {
        let payload = "x".repeat(10_000);
        let registry = Registry::default();

        let tg = registry
            .build(Kind::Telegram, &cx("telegram://1", "t", payload.as_bytes()))
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&tg.body).unwrap();
        assert_eq!(v["text"].as_str().unwrap().len(), TELEGRAM_MAX_TEXT);

        let dc = registry
            .build(Kind::Discord, &cx("discord://1", "t", payload.as_bytes()))
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&dc.body).unwrap();
        // Two platforms, two limits. Sharing one number would either waste half of
        // Telegram's allowance or have Discord reject every long message.
        assert_eq!(v["content"].as_str().unwrap().len(), DISCORD_MAX_CONTENT);
    }

    #[test]
    fn truncation_lands_on_a_character_boundary() {
        // A cut through the middle of a multi-byte character produces invalid UTF-8,
        // which the platform rejects as a malformed body — turning a long message into
        // a failed delivery for a reason nobody would guess from the error.
        let payload = "é".repeat(4000);
        let out = Registry::default()
            .build(Kind::Discord, &cx("discord://1", "t", payload.as_bytes()))
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out.body).unwrap();
        let content = v["content"].as_str().unwrap();
        assert!(content.len() <= DISCORD_MAX_CONTENT);
        assert!(std::str::from_utf8(content.as_bytes()).is_ok());
    }

    #[test]
    fn a_malformed_address_is_refused_at_registration() {
        let r = Registry::default();
        for bad in [
            "",
            "-100123",
            "https://example.com",
            "telegram://",
            "telegram://a/b",
        ] {
            assert!(
                r.validate(Kind::Telegram, bad).is_err(),
                "{bad:?} should be refused"
            );
        }
        assert!(r.validate(Kind::Telegram, "telegram://-100123").is_ok());
        assert!(r.validate(Kind::Telegram, "telegram://@mychannel").is_ok());
    }

    #[test]
    fn a_discord_id_must_be_digits_only() {
        let r = Registry::default();
        assert!(r.validate(Kind::Discord, "discord://998877665544").is_ok());
        // Not fussiness: anything else could smuggle a path segment into the URL this
        // builds, which is how a webhook id becomes a request to somewhere else.
        for bad in [
            "discord://",
            "discord://abc",
            "discord://12/../34",
            "discord://1?x=2",
        ] {
            assert!(
                r.validate(Kind::Discord, bad).is_err(),
                "{bad:?} should be refused"
            );
        }
    }

    #[test]
    fn every_kind_round_trips_through_its_stored_name() {
        // The column holds these strings, so a rename here is a migration there.
        for kind in [Kind::Http, Kind::Telegram, Kind::Discord] {
            assert_eq!(Kind::parse(kind.as_str()), Some(kind));
        }
        assert_eq!(Kind::parse("slack"), None);
    }

    #[test]
    fn a_test_registry_only_moves_the_chat_bases() {
        // The HTTP transport uses the customer's own URL and has no base to move, so
        // pointing the chat APIs at a stand-in must not change it.
        let r = Registry::with_bases("http://localhost:1", "http://localhost:1");
        let out = r
            .build(Kind::Http, &cx("https://example.com/hook", "k", b"{}"))
            .unwrap();
        assert_eq!(out.url, "https://example.com/hook");
    }
}
