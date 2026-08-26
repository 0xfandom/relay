//! A body extractor that preserves the exact bytes received.
//!
//! Why this exists at all: axum's `Json<T>` extractor deserialises the body into a
//! struct, and the original bytes are gone. Serialising that struct again later
//! produces *equivalent* JSON with possibly different bytes — key order is not
//! defined — and the HMAC signature covers bytes, not meaning. One re-serialisation
//! anywhere in the path and every signature we send fails verification.
//!
//! So the ingest path takes the body as raw bytes, stores them verbatim, and signs
//! those same bytes at send time.
//!
//! This is also the first trait implementation in the project. `FromRequest` is
//! axum's contract for "a type that can be built from an incoming request"; any
//! type implementing it can appear as a handler argument.

use axum::{
    body::Bytes,
    extract::{FromRequest, Request},
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::AppState;

/// Default maximum accepted body size. Without a cap, one large request can exhaust
/// memory — the body is buffered before the handler ever runs.
pub const MAX_BODY_BYTES: usize = 256 * 1024;

pub struct RawBody(pub Bytes);

pub enum RawBodyRejection {
    /// Carries the cap it exceeded, so the caller is told what to fit inside rather
    /// than left to guess. The number is configurable, so a constant in the message
    /// would eventually be a lie.
    TooLarge(usize),
    Unreadable,
}

impl IntoResponse for RawBodyRejection {
    fn into_response(self) -> Response {
        match self {
            Self::TooLarge(cap) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("body exceeds {cap} bytes"),
            ),
            Self::Unreadable => (
                StatusCode::BAD_REQUEST,
                "could not read request body".to_string(),
            ),
        }
        .into_response()
    }
}

/// Bound to [`AppState`] rather than generic over any state.
///
/// It has to read the configured cap from somewhere, and taking it from the state
/// the router already carries is better than a process-global: a test can vary it,
/// and the value the handler enforces is visibly the same one the binary configured.
impl FromRequest<AppState> for RawBody {
    type Rejection = RawBodyRejection;

    async fn from_request(req: Request, state: &AppState) -> Result<Self, Self::Rejection> {
        let cap = state.max_body_bytes;
        // Reject on the declared length before reading anything, so an oversized
        // body is refused rather than buffered.
        if let Some(len) = req
            .headers()
            .get(axum::http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
            && len > cap
        {
            return Err(RawBodyRejection::TooLarge(cap));
        }

        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|_| RawBodyRejection::Unreadable)?;

        // A missing or lying Content-Length is still caught here.
        if bytes.len() > cap {
            return Err(RawBodyRejection::TooLarge(cap));
        }

        Ok(RawBody(bytes))
    }
}
