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

## Sending an event more than once

A producer whose `POST /v1/events` times out cannot tell whether Relay received it.
Not retrying loses the event; retrying creates a second one. Name the request and
the ambiguity goes away:

```bash
curl -X POST 127.0.0.1:8080/v1/events \
  -H 'content-type: application/json' \
  -H 'idempotency-key: order-123-paid' \
  -d '{"type":"order.paid","amount":4999}'
```

The first request with a given key creates the event and stores the exact bytes of
its `202` body. Every later request with that key creates nothing and is answered
with those same bytes, so a caller comparing two responses gets equality rather
than two different event ids. Duplicates carry `Relay-Idempotent-Replay: true`.

Concurrency is handled by the database, not by application code: the key is a
primary key, the event and its fan-out are inserted in the same transaction that
claims it, and a losing insert rolls all of it back and returns the winner's
response. A hundred simultaneous identical requests produce one event, one delivery
per endpoint, a hundred identical bodies and no `5xx`.

Two rules worth knowing before relying on it:

- **The key is scoped to the request.** Reusing one key for a different event type
  or body is `409 Conflict`, not a silent replay. Being answered with the earlier
  event's id would drop the second event while reporting success.
- **Keys expire after 24 hours** (`RELAY_IDEMPOTENCY_RETENTION_SECS`). A duplicate
  arriving after that creates a second event. Keeping keys forever would grow that
  table as fast as the event table to answer a question nobody asks after the first
  hour.

Without the header nothing is deduplicated, which is deliberate: two identical
bodies a second apart may be a retry or two real orders, and only the producer
knows which.

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

## At-least-once, and what receivers must do

Relay sends a webhook and no reply comes back. Two things can have happened: the
endpoint never received it, or it received it, processed it, and the acknowledgement
was lost. Those are indistinguishable from our side — permanently, not merely in
practice. Exactly-once delivery over a network we do not control is not achievable,
and no amount of design changes that.

Given the choice, Relay retries. Losing an event is worse than sending one twice.
That makes the guarantee **at-least-once**: a receiver will occasionally see the
same webhook more than once, and it is expected to cope.

`Relay-Delivery-Id` is what makes coping possible. It is fixed when the delivery row
is created and repeated on every attempt, so:

```
store the ids you have processed
on each webhook:
    if the id is already stored -> acknowledge and stop
    otherwise                   -> process, store the id, acknowledge
```

Three properties receivers can rely on:

- **Constant across retries.** Attempt 1 and attempt 8 of one delivery carry the
  same id. If it changed per attempt, every retry would look like a new event.
- **Constant across replays.** Draining the dead letter queue retries *that*
  delivery rather than creating a new one, so a receiver that already processed it
  will correctly ignore the replay. The generation counter moves; the id does not.
- **One per endpoint, not per event.** An event fanning out to three endpoints
  produces three ids, so two endpoints sharing a deduplication store never discard
  each other's webhooks.

The id does not prevent duplicates. It makes them detectable, which is the strongest
thing anyone can offer.

Storing every id forever is not required — Relay stops retrying a delivery once it
is dead, so a receiver only needs to remember ids for as long as retries can still
arrive.

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
