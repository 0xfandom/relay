//! Relay's pure domain logic.
//!
//! Hard rule for this crate: **no I/O**. No database, no HTTP, no `async`,
//! no clock reads. Everything here is a function from values to values, so it
//! tests in microseconds and can never be flaky.
//!
//! That constraint is enforced by the dependency list in `Cargo.toml` — if you
//! ever find yourself wanting `sqlx` or `tokio` in here, the code belongs in a
//! different crate.

pub mod backoff;
pub mod idempotency;
pub mod outcome;
pub mod rate_limit;
pub mod signature;
pub mod url_guard;
