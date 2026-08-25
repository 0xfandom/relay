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
| Endpoint that hangs, never replies | Per-endpoint in-flight cap, so other customers flow |
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
  metrics/     every metric name, and the /metrics endpoint that renders them
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

## How fast Relay sends

Every other protection here reacts to a failure. The rate limit prevents one — and
the failure it prevents is one Relay causes. A customer subscribes to a high-volume
event, one burst fans out into ten thousand deliveries, their server falls over, and
every one of those then fails, retries, and arrives again as a wave.

Each endpoint has a sustained rate and a burst allowance:

```bash
curl -X POST 127.0.0.1:8080/v1/endpoints \
  -H 'content-type: application/json' \
  -d '{"url":"https://example.com/hook","rate_per_second":25,"burst":50}'
```

Defaults are 10/s with a burst of 20 — deliberately conservative, since Relay cannot
know what a customer's server can take and the cost of guessing high is their
outage.

**A bucket, not a window.** Counting requests per fixed second is wrong at the
boundary: ten at `t=0.99` and ten more at `t=1.01` are two legal seconds and twenty
requests in twenty milliseconds. A bucket has no boundaries — tokens accrue
continuously and are capped at `burst`, so the most that can ever leave at once is
`burst`, however the traffic lines up with a clock.

**A deferral is not a failure.** A delivery with no token available goes back to
`pending`, scheduled for when a token will exist, and its attempt counter is left
alone. This is the part that matters: if throttling spent an attempt, a busy
endpoint's deliveries would reach the dead letter queue having never had a single
request made to them — a retry budget consumed entirely by our own throttle.
Deferrals are still written to the attempt log with the class `deferred`, because
"held back for 300ms" is what someone asking why a webhook was late needs to see.

Buckets live in the dispatcher process, so two dispatcher replicas each keep their
own and the effective rate doubles. The fix is a shared bucket rather than a
different algorithm; the arithmetic does not change.

## When Relay stops knocking

Retries and rate limits both assume the endpoint is worth talking to. The circuit
breaker is the case where it is not: the server has been down for an hour, every
delivery will time out, and each one costs a worker the full request timeout to learn
what the last thousand already established.

```
           5 consecutive failures
  Closed ─────────────────────────▶ Open
    ▲                                │
    │ probe succeeds     cooldown expires
    │                                ▼
    └──────────── HalfOpen ◀─────────┘
                     │
                     └── probe fails ──▶ Open (longer cooldown)
```

`HalfOpen` is what earns the design its keep. Without it a breaker that opens never
closes, because nothing is ever tried again.

**The question is "did the endpoint answer", not "did this succeed."**

| Outcome | Reading |
| --- | --- |
| any status the server sent, `404` and `429` included | alive |
| `5xx` | failing — the server reporting its own fault |
| timeout, connection refused | failing — nothing answered |
| unparseable URL, refused address | no evidence either way |

A stream of `404`s is a misconfigured URL and a `429` is a working server asking us
to slow down. Both servers are up. Tripping on either cuts off a destination that was
fine while hiding a problem that needs a person.

**State lives on the endpoint row, not in process memory.** This is the difference
between a breaker that works and one that looks like it does: held in memory it is
correct with a single worker and silently fails with several, because each sees a
fraction of the failures, none reaches the threshold, and every worker independently
concludes the endpoint is merely unlucky. Recording it is a locked read-modify-write,
so two workers reporting a failure at the same instant count as two.

Like the other deferrals, being held behind an open breaker spends no attempt — and
this one matters most, because charging attempts for the time an endpoint is cut off
would empty every pending delivery's retry budget during the outage and they would
all be dead by the time it came back.

**Exactly one worker probes.** When a cooldown expires it has expired for every
worker holding a delivery to that endpoint, and a server that has just come back
after an hour down will very likely fall over again if met by the whole backlog at
once — at which point the breaker reopens with a longer cooldown and the outage
extends itself. The probe is claimed with a single conditional `UPDATE ... RETURNING`
so the database picks the winner; a read followed by a write would let every worker
decide it was the prober.

The claim also writes a deadline. A probe against an endpoint that accepts
connections and never answers would otherwise leave the breaker half-open forever
with nobody allowed to try again — a permanent outage produced by the thing meant to
end one.

Everyone else waits briefly rather than for that deadline, because a probe settles
the question within one request timeout and whichever way it goes they want to know.

Cooldowns double per consecutive trip and cap at five minutes.
`RELAY_BREAKER_THRESHOLD`, `RELAY_BREAKER_COOLDOWN_SECS` and
`RELAY_BREAKER_MAX_COOLDOWN_SECS` configure it; `RELAY_BREAKER=false` disables it.

## One endpoint cannot take the pool with it

An endpoint that accepts connections and then never replies is the worst kind of
failure: nothing errors, nothing retries, the workers simply stop coming back for a
full request timeout. With thirty-two workers and one such endpoint holding a deep
backlog, every other customer's webhooks wait behind a server that is not even
answering.

Two caps, protecting different people:

| Cap | Default | Protects |
| --- | --- | --- |
| `RELAY_MAX_PER_ENDPOINT` | 8 | other customers — the bulkhead |
| `RELAY_MAX_IN_FLIGHT` | 64 | Relay's own sockets and memory |

The global cap is deliberately independent of the worker count. A worker spends most
of its life waiting on someone else's server, so "how many tasks" and "how many open
sockets" are not the same question.

The two are acquired differently, and that difference is the bulkhead:

- **Per-endpoint is non-blocking.** No slot free means the delivery is deferred, and
  the worker is free again within microseconds. Waiting here would tie a worker to an
  endpoint that has stopped answering — precisely the coupling the cap exists to
  break. The deferral delay is short and jittered, so a saturated endpoint's backlog
  does not return in a single wave.
- **Global is blocking.** Its holders are all actively sending, so a waiter is
  waiting on work that is definitely progressing, and no single endpoint can
  monopolise it because the per-endpoint cap already bounds any one endpoint's share.

Like a rate-limit deferral, hitting either cap spends no attempt: nothing was sent,
so nothing was learned about whether the endpoint works.

Permits are released by `Drop`, so a task that panics mid-request returns its slots
while unwinding. That has its own test, because the failure mode is invisible until
the process has slowly leaked its whole allowance and quietly stops sending.

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

## What Relay reports about itself

Five separate mechanisms can now hold a delivery back — the backoff, the endpoint
rate limit, the global and per-endpoint concurrency caps, and the breaker — and
from outside the process none of them was visible. Both binaries export Prometheus
metrics; the API on its own port at `/metrics`, the dispatcher on
`RELAY_METRICS_BIND` (default `0.0.0.0:9091`).

```bash
curl -s localhost:8080/metrics   # ingest
curl -s localhost:9091/metrics   # queue, deliveries, breakers
```

Two scrape targets rather than one, because they are two processes. The queue
gauges are exported by the dispatcher *only*: they describe rows in a database both
processes share, so a second reporter would appear as a duplicate series under a
different `instance` label and any dashboard summing across instances would report
twice the queue that exists.

The number to watch first is `relay_queue_oldest_pending_age_seconds`, not
`relay_queue_depth`. Depth lies in both directions — it can be thousands and
perfectly healthy while a burst drains, or three and catastrophic when those three
have been stuck for an hour. Age only goes up when something is genuinely not
moving. It reports `NaN` rather than `0` on an empty queue, because "nothing is
queued" and "the oldest item is zero seconds behind" are both healthy and a panel
that cannot tell them apart is a panel nobody checks.

`relay_delivery_attempts_total` uses the same four outcome names the attempt log
stores in `outcome_class`, so a number on a graph and a row in the database can be
reconciled without a translation table. Deferrals are broken down separately by
`relay_deliveries_deferred_total{reason}`: during an incident the question is never
"how much is being deferred" but "by which gate".

Every counter is reported at zero on startup, including every label value it can
ever carry. Without that a counter that has never fired is simply absent from the
scrape, and an empty panel reads exactly like a broken exporter — so the healthiest
possible state would be indistinguishable from no data at all.

## License

MIT
