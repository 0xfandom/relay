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

/// Maximum accepted body size. Without a cap, one large request can exhaust
/// memory — the body is buffered before the handler ever runs.
pub const MAX_BODY_BYTES: usize = 256 * 1024;

pub struct RawBody(pub Bytes);

pub enum RawBodyRejection {
    TooLarge,
    Unreadable,
}

impl IntoResponse for RawBodyRejection {
    fn into_response(self) -> Response {
        match self {
            Self::TooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("body exceeds {MAX_BODY_BYTES} bytes"),
            ),
            Self::Unreadable => (
                StatusCode::BAD_REQUEST,
                "could not read request body".to_string(),
            ),
        }
        .into_response()
    }
}

impl<S> FromRequest<S> for RawBody
where
    S: Send + Sync,
{
    type Rejection = RawBodyRejection;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        // Reject on the declared length before reading anything, so an oversized
        // body is refused rather than buffered.
        if let Some(len) = req
            .headers()
            .get(axum::http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
            && len > MAX_BODY_BYTES
        {
            return Err(RawBodyRejection::TooLarge);
        }

        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|_| RawBodyRejection::Unreadable)?;

        // A missing or lying Content-Length is still caught here.
        if bytes.len() > MAX_BODY_BYTES {
            return Err(RawBodyRejection::TooLarge);
        }

        Ok(RawBody(bytes))
    }
}
