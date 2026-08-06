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
cargo test
cargo test -p relay-domain
```

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

## License

MIT
