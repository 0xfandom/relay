//! How Relay's logs are formatted, in one place for both binaries.
//!
//! It sits in this crate rather than in a seventh one because it is the same
//! concern: what Relay says about itself, in a shape something else can read. And
//! it has to be shared rather than copied, because the point of the spans in the
//! dispatcher is that a delivery's path is reconstructable — which stops being true
//! the moment the two processes disagree about whether spans are attached, what the
//! timestamp format is, or where the output goes.
//!
//! The format is chosen by looking at where stderr points, not by an environment
//! variable that somebody has to remember. A container's stderr is a pipe into a log
//! collector that wants JSON; a developer's stderr is a terminal, where JSON is
//! unreadable. Getting this wrong by default means either production logs that
//! cannot be queried or a local run that cannot be read, and both are the kind of
//! thing that gets discovered at the worst moment.

use std::io::IsTerminal;

use tracing_subscriber::{EnvFilter, fmt, fmt::format::FmtSpan};

/// Install the global subscriber.
///
/// `RUST_LOG` selects verbosity as usual. `RELAY_LOG_FORMAT` forces `json` or
/// `text` when the automatic choice is wrong — piping a local run through `jq`, or
/// reading a container's logs by eye.
pub fn init() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if json_output() {
        fmt()
            .json()
            // Both are off by default and both are the point. Without the span list
            // an event logged deep inside a delivery carries no delivery id, and
            // reconstructing which of five hundred concurrent deliveries it belonged
            // to is impossible — which is the entire reason the spans exist.
            .with_current_span(true)
            .with_span_list(true)
            // One line when a span closes, carrying how long it was busy. This is
            // what turns "the delivery took nine seconds" into "eight and a half of
            // them were inside `send`", without a tracing backend to ship spans to.
            //
            // Cheap at the default level by construction: the stages that would be
            // noisy — the gate, the claim, the write — are `debug_span!`, so at
            // `info` they are disabled and cost nothing at all.
            .with_span_events(FmtSpan::CLOSE)
            .with_env_filter(filter)
            .init();
    } else {
        fmt().with_env_filter(filter).with_target(false).init();
    }
}

/// Whether to emit JSON.
fn json_output() -> bool {
    match std::env::var("RELAY_LOG_FORMAT").as_deref() {
        Ok("json") => true,
        Ok("text") => false,
        // Anything else, including nothing: decide from where the output is going.
        _ => !std::io::stderr().is_terminal(),
    }
}
