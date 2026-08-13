# Relay

An outbound webhook delivery service in Rust.

Applications hand Relay an event. Relay signs it, queues it, and delivers it to
every subscribed endpoint — retrying with backoff, tripping circuit breakers on
dead endpoints, rate limiting slow ones, and recording every attempt.

## What it handles

Sending one HTTP POST is trivial. Everything that makes this a system is what
happens when the POST fails — and against thousands of servers on the open
internet, it fails constantly.

| Failure | Response |
| --- | --- |
| Endpoint down for hours | Retry with jittered exponential backoff |
| Endpoint never returns | Dead letter queue, replayable |
| Large event bursts | Durable queue with a worker pool |
| Slow endpoint | Per-endpoint token bucket rate limiting |
| Dead endpoint, deep backlog | Circuit breaker with probing recovery |
| Forged requests | HMAC-SHA256 signatures over the raw body |
| Duplicate delivery | Idempotency keys and a stable delivery id |
| Delivery disputes | Append-only attempt log with status and latency |

## Design

- **At-least-once delivery** with a delivery id that is stable across retries,
  so receivers can deduplicate. Exactly-once over an untrusted network is not
  achievable; retries are made safe rather than rare.
- **Postgres as the queue** — `FOR UPDATE SKIP LOCKED` with leases and a reaper
  for crash recovery. One transactional store provides durability and atomic
  state without a separate broker.
- **Two processes** — an ingest API that accepts and returns `202` immediately,
  and a dispatcher that drains the queue. They interact only through the
  database, and either can restart independently.
- **Pure core** — logic that can be free of I/O (signing, backoff, breaker state
  machine, outcome classification) lives in a crate with no `async` and no
  database, so it is deterministic and fast to test.

## Layout

```
crates/
  domain/      pure logic, no I/O — signing, backoff, breaker state machine
  store/       sqlx repositories and migrations
  dispatcher/  worker pool, reaper, HTTP sender
  api/         axum ingest and admin endpoints
  testkit/     configurable receiver for integration tests
```

## Build

Requires a recent stable Rust toolchain.

```bash
docker compose up -d       # Postgres on 5433
export DATABASE_URL=postgres://relay:relay@localhost:5433/relay
cargo test                 # unit tests plus end-to-end against a real database
```

`DATABASE_URL` is required: the store's tests create a throwaway database each, so
that concurrent workers in one test cannot claim rows belonging to another.

## Running locally

Three processes: the ingest API, the dispatcher, and a configurable receiver
standing in for a customer endpoint.

```bash
export DATABASE_URL=postgres://relay:relay@localhost:5433/relay
# The receiver below runs on loopback, which the dispatcher refuses by default.
export RELAY_ALLOW_PRIVATE_ENDPOINTS=true

RELAY_TESTKIT_SECRET=whsec_demo cargo run -p relay-testkit   # :9090
cargo run -p relay-api                                       # :8080
cargo run -p relay-dispatcher
```

Register an endpoint and send it an event:

```bash
curl -X POST 127.0.0.1:8080/v1/endpoints \
  -H 'content-type: application/json' \
  -d '{"url":"http://127.0.0.1:9090/verify","event_types":["order.paid"]}'

curl -X POST 127.0.0.1:8080/v1/events \
  -H 'content-type: application/json' \
  -d '{"type":"order.paid","amount":4999}'
# 202 Accepted, with the event id and one delivery id per subscribed endpoint
```

The receiver exposes failure modes for testing delivery behaviour:
`/always500`, `/slow?ms=`, `/flaky?pct=`, `/429?retry_after=`, and `/verify`,
which checks the signature and the freshness window.

## Signature format

Modelled on Stripe's scheme, so existing receiver implementations apply.

```
Relay-Timestamp:   1700000000
Relay-Signature:   v1=<hex>,v1=<hex>      # current and previous secret
Relay-Delivery-Id: <uuid>                 # stable across retries
```

The signed string is exactly `<timestamp>.<raw body bytes>`. Signing the raw
bytes matters: re-serialising JSON can reorder keys and invalidate every
signature. Two signatures are sent during secret rotation so endpoints can
migrate without failed deliveries.

## Where deliveries may go

Relay makes an HTTP request to whatever URL a customer registers, from a machine
that sits inside a private network. Left unguarded that is a server-side request
forgery engine: an endpoint registered as
`http://169.254.169.254/latest/meta-data/iam/security-credentials/` would be fetched
from inside the instance, where the cloud metadata service answers without
authentication, and the stored response snippet would carry the credentials back out
through the delivery history.

So the dispatcher refuses any URL that is not `http`/`https`, and any host that
resolves into loopback, private, link-local, carrier-NAT, multicast or reserved
space — checked against the resolved address rather than the URL text, because
loopback has too many spellings to blocklist. The check happens at send time, not
only at registration, since a domain that is public today can be repointed
tomorrow. Redirects are not followed, and a refused delivery stores no response
body.

`RELAY_ALLOW_PRIVATE_ENDPOINTS=true` disables the address check for local
development. It is off by default and should stay off anywhere real.

## License

MIT
