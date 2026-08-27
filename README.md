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
  dispatcher/  worker pool, sender, reaper, pruner, heartbeat, outbox publisher
  api/         axum ingest and admin endpoints
  metrics/     every metric name, and the /metrics endpoint that renders them
  testkit/     configurable receiver for integration tests
  loadtest/    load generator and configuration sweep
  broker/      Redis Streams transport behind a trait, for running a fleet
docs/          deployment, configuration, receiver integration, load test results
ops/           Prometheus and Grafana provisioning
Dockerfile     one builder, three runtime images
```

## Running the whole thing

One command, from a clean machine to a working stack — API, dispatcher, Postgres,
Prometheus, Grafana, and a receiver to deliver to.

```bash
docker compose up --build
curl -s localhost:8080/readyz | jq
```

The full walkthrough, every configuration variable, and what a real receiver has to
implement are in [docs/deployment.md](docs/deployment.md).

## Building and testing

Requires a recent stable Rust toolchain. Compiling inside Docker on every edit is the
slow loop, so run Postgres in a container and the code on the host:

```bash
docker compose up -d postgres
export DATABASE_URL=postgres://relay:relay@localhost:5433/relay
cargo test                 # unit tests plus end-to-end against a real database
```

`DATABASE_URL` is required: the store's tests create a throwaway database each, so
that concurrent workers in one test cannot claim rows belonging to another.

## Running the processes by hand

Three processes: the ingest API, the dispatcher, and a configurable receiver
standing in for a customer endpoint.

```bash
export DATABASE_URL=postgres://relay:relay@localhost:5433/relay
# The receiver below is on loopback, over plain HTTP, on an odd port — all three
# refused by default. This is the one switch that relaxes the whole posture, and
# both processes read it, so registration and delivery agree.
export RELAY_ALLOW_PRIVATE_ENDPOINTS=true

RELAY_TESTKIT_SECRET=whsec_demo cargo run -p relay-testkit   # :9099
cargo run -p relay-api                                       # :8080
cargo run -p relay-dispatcher
```

Register an endpoint and send it an event:

```bash
curl -X POST 127.0.0.1:8080/v1/endpoints \
  -H 'content-type: application/json' \
  -d '{"url":"http://127.0.0.1:9099/verify","event_types":["order.paid"]}'

curl -X POST 127.0.0.1:8080/v1/events \
  -H 'content-type: application/json' \
  -d '{"type":"order.paid","amount":4999}'
# 202 Accepted, with the event id and one delivery id per subscribed endpoint
```

The endpoint's signing secret is returned once, at creation. Hand it to the receiver
with `curl -X POST 127.0.0.1:9099/secret --data '<secret>'`, or it will answer `401`.

The receiver exposes failure modes for testing delivery behaviour: `/always500`,
`/slow?ms=`, `/flaky?pct=`, `/429?retry_after=`, `/bigbody?kb=`, `/trickle?ms=`
(answers `200` and then dribbles its body forever), `/toggle`, and `/verify`, which
checks the signature and the freshness window.

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

## Where a delivery can go

An endpoint has a transport. It decides how the request is built and **nothing
else** — the same retries, backoff, breaker, rate limit and attempt log apply to all
three. That is the test of whether the abstraction sits in the right place: if a
transport ever needed to change retry or breaker behaviour, it wouldn't.

| Transport | `url` holds | `secret` holds | Body | Signed |
| --- | --- | --- | --- | --- |
| `http` | the customer's URL | the signing key | their bytes, verbatim | yes |
| `telegram` | `telegram://<chat_id>` | the bot token | `{"chat_id", "text"}` | no |
| `discord` | `discord://<webhook_id>` | the webhook token | `{"content"}` | no |

```bash
curl -X POST 127.0.0.1:8080/v1/endpoints \
  -H 'content-type: application/json' \
  -d '{"url":"telegram://-1001234567890","transport":"telegram",
       "secret":"123456:AA...","event_types":["order.paid"]}'
```

**The credential never goes in the URL.** Telegram's bot token and Discord's webhook
token are both path segments in their native form — `https://api.telegram.org/bot<TOKEN>/sendMessage`
— and a URL is returned by the admin API, stored on every dead letter, and written
into a span on every send. Storing one there would leak it into three places at once,
none of which anyone would think to redact. So an endpoint stores its *address* in
`url` and its *credential* in `secret`, which is already the redacted type.

The real URL is assembled at send time, used, and never written down. `Outbound`
carries a `display_url` alongside it with the credential replaced, and that is the
one the span and the logs get — two fields rather than a redacting function called at
each log site, for the same reason `Secret` has no `Display`.

**Signing is a property of the transport.** The HMAC exists so a receiver can prove a
payload came from us. Telegram already knows it did, because the request arrived
carrying our bot token, so a signature there is ceremony. Only `http` signs.

**Chat messages are text, so the payload is rendered rather than forwarded.** This is
the one place Relay's never-re-encode rule does not apply — and it is allowed not to
apply precisely because nothing here is signed. The event type leads the message,
since in a channel carrying four kinds of event that is the first thing anyone needs.
Each platform's own limit is respected (Telegram 4096, Discord 2000) by truncating on
a character boundary: a message that arrives cut short is worth more than one that
does not arrive.

The SSRF policy is applied to the **built** URL, not the stored address — for a chat
transport those are different strings, and only the built one can be resolved.

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

So a delivery has to clear four things before a connection is opened:

| Check | Default | Why |
| --- | --- | --- |
| Scheme | `https` only | The signature proves who sent a payload and that nobody changed it. It does nothing to keep it private. |
| Port | `443` | A public address on an arbitrary port is still a port scanner. An endpoint URL is a place to receive webhooks. |
| Address | Public ranges only | Loopback, private, link-local, carrier-NAT, multicast and reserved space are all refused. |
| Redirects | Not followed | A `302` is the easiest way for a URL that passed validation to end up somewhere else. |

The address is checked against what the hostname *resolves to*, never against the
URL text — loopback has far too many spellings (`127.1`, `0.0.0.0`, `2130706433`,
`0x7f000001`, `::ffff:127.0.0.1`) for a string blocklist to catch them all. And the
check lives inside the HTTP client's own DNS resolver, so the address that was
approved is the address connected to; checking first and connecting second lets a
resolver under the attacker's control answer honestly once and dishonestly once.

A refused delivery is recorded with its own dead-letter reason, `refused`, kept
apart from `permanent_failure`. The two need different responses from a person: a
permanent failure is a customer's broken URL, a refusal is somebody pointing an
endpoint at an address they should not — which is worth an alert before they try the
next spelling.

`RELAY_ALLOW_PRIVATE_ENDPOINTS=true` is the development switch, and the other two
rules follow it unless set explicitly: a laptop's receivers are on loopback, over
plain HTTP, on whatever port the OS handed out. Needing three variables to say "this
is a laptop" means somebody eventually sets the first one in production to make an
error go away. `RELAY_REQUIRE_HTTPS` and `RELAY_ALLOWED_PORTS` (a comma-separated
list, or `*`) override it either way.

Both processes build this policy from the same variables. Registration is only a
courtesy check — the authority is the send path — but a courtesy check that
disagrees with the authority is worse than none, because it accepts URLs that will
never deliver and the caller finds out from the dead letter queue.

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

## The dashboard

```bash
docker compose up -d      # Postgres, Prometheus, Grafana
cargo run -p relay-api &
cargo run -p relay-dispatcher &
open http://localhost:3000
```

That is the whole setup. The datasource and the dashboard are provisioned from
`ops/`, anonymous access is on, and Grafana opens straight onto the dashboard —
a login prompt is manual configuration, which is the thing this is meant not to
need. Prometheus scrapes both Relay processes on the host rather than running them
in containers, because rebuilding an image on every edit to watch a graph move is
not a development loop.

The panels are grouped by the failure mode they make visible, so a chaos scenario
run by hand is identifiable from the dashboard alone:

| Row | Reads |
| --- | --- |
| Is Relay healthy | Oldest pending age, due now, dead letters, endpoints cut off |
| Back-pressure (M5) | Deferrals split by which gate held them, delivery latency |
| Retries and dead letters (M3) | Failures by class, deaths by reason |
| Breakers (M6) | Endpoints by state, trips against probes that recovered |
| Workers (M2) | In flight, and deliveries rescued from dead workers |
| Ingest (M4) | Accepted against replayed, ingest latency against delivery latency |

Two panels are there to be read as negatives. *Deliveries rescued from dead
workers* should be flat at zero — any slope at all is a report that something
upstream is crashing, and nowhere else shows it. *Ingest latency against delivery
latency* should be two unrelated lines: if ingest ever starts tracking delivery,
the two paths have become coupled and the API has inherited a customer's outage.

The dashboard is read-only in the UI. One edited in Grafana and not written back to
the repository disappears the next time the container is recreated, which is worse
than not being able to edit it.

## Rotating a signing secret

```bash
curl -sX POST localhost:8080/v1/endpoints/$ENDPOINT_ID/rotate-secret
# {"id":"...","secret":"whsec_...","previous_secret_expires_at":"2026-08-27T09:00:00Z"}
```

A single-secret rotation is a cutover, and there is no ordering of the two changes
that avoids failed deliveries. If Relay switches first, every receiver still
checking the old secret rejects us. If the receiver switches first, they reject us
until we catch up. The customer cannot fix that by deploying faster.

So both secrets sign during an overlap window and the `Relay-Signature` header
carries both, comma-separated:

```
Relay-Signature: v1=<new>,v1=<old>
```

A receiver that matches on *any* entry — which is what the format has always asked
for — can switch at any moment inside the window with nothing failing on either
side. `RELAY_SECRET_OVERLAP_SECS` sets the window, 24 hours by default: it has to
be long enough for the customer to notice, change a value and roll a fleet, or it
expires mid-migration and causes the outage it exists to prevent.

Rotating twice inside one window discards the secret from the first rotation. That
is deliberate — "how many keys can sign as you" is exactly the number a rotation
exists to keep at one, and a chain of previous secrets would let it grow without
limit. The header therefore never carries more than two.

The old secret stops being sent because the query stops selecting it, not because
anything sweeps it. A cleanup job that quietly died must not be able to keep a
retired secret alive.

Secrets are a `Secret` type with no `Display` at all — `{secret}` does not compile —
and a `Debug` that prints `<redacted>`. Reading the bytes takes `expose()`, which is
awkward to type and trivial to grep for at review. The failure mode of the
alternative is one log line written during an incident, after which every customer
has to be told to change their verification key.

## Asking what happened to an event

```bash
# One delivery and every attempt made on it
curl -s localhost:8080/v1/deliveries/$DELIVERY_ID

# An endpoint's history, newest first
curl -s "localhost:8080/v1/endpoints/$ENDPOINT_ID/deliveries?status=dead&limit=50"
```

Paged by position, not by `OFFSET`. `OFFSET n` makes the database walk and discard
`n` rows before returning anything, so page one is instant and page four hundred
reads forty thousand rows to produce a hundred — on the largest and fastest-growing
table in the system. It is also wrong under concurrent writes: a delivery created
between two requests shifts every later row down by one, so the reader sees a row
twice and never sees another at all.

Each page instead carries its last row's position forward as an opaque `next_cursor`,
which makes every page an index seek to a known place. Page four hundred costs what
page one does, and an insert somewhere else in the ordering cannot move it.

The position is `(created_at, id)`, not `created_at` alone. A fan-out writes every
delivery for one event in a single transaction, so timestamps are routinely tied,
and paging on a non-unique key skips and repeats rows at every boundary.

An unknown endpoint is a `404` rather than an empty page, and an unrecognised status
filter is a `400` naming the four that work. Both for the same reason: an empty page
reads as "this endpoint has had no failures", which is the most reassuring answer
there is and the wrong one to give somebody who has pasted the wrong id.

Every query is scoped to one endpoint in the store's own `WHERE` clause rather than
filtered in the handler, so a route added later cannot forget to apply it. The
endpoint is Relay's ownership boundary today; when tenants exist, the tenant
predicate joins it there.

## What one endpoint can make Relay spend

Every bound on a single outbound request, all configurable, all with defaults that
are safe rather than generous:

| Limit | Default | Env |
| --- | --- | --- |
| Connect timeout | 5s | `RELAY_CONNECT_TIMEOUT_SECS` |
| Read timeout | 5s | `RELAY_READ_TIMEOUT_SECS` |
| Total timeout | 10s | `RELAY_REQUEST_TIMEOUT_SECS` |
| Payload sent | 256 KiB | `RELAY_MAX_PAYLOAD_BYTES` |
| Response body read | 2 KiB | `RELAY_MAX_RESPONSE_BYTES` |

Three timeouts rather than one, because they fail differently. The read timeout
catches a connection that goes silent — abandoned in seconds instead of occupying a
worker for the whole budget. It cannot replace the total timeout, because **it
resets on every byte**: an endpoint answering `200` and then dribbling its body one
byte every fifty milliseconds satisfies a read timeout indefinitely. That is the
classic way a worker pool dies, and only the total timeout ends it. There is a
`/trickle` route in the testkit and a test that points Relay at it.

The response body is streamed and stopped at the cap, then the connection is
dropped without draining it. Reading to the end and truncating afterwards produces
the identical stored snippet and costs the whole eight-megabyte error page, so the
test asserts the *bytes actually read from the network*, not the length of what was
stored.

The lease TTL is validated against the configured total timeout at startup, not
against a constant. The two are set independently, and a lease that can expire
mid-request hands the row to a second worker — the endpoint gets the webhook twice
and nothing reports why.

`RELAY_MAX_PAYLOAD_BYTES` bounds both ends: ingest refuses a larger body with a
`413`, and the dispatcher refuses to send one. The second is unreachable in normal
operation and exists for the case that is not normal — a cap lowered after rows were
already stored under the old one, which would otherwise retry forever.

## Keeping the database from growing forever

The attempt log is the only table that grows with *traffic* rather than with
customers: every delivery writes a row and a failing one writes twelve. At any real
volume it is the largest object in the database by an order of magnitude.

The obvious retention is `DELETE FROM delivery_attempts WHERE at < now() - 30 days`,
and it is the wrong tool by a wide margin. Deleting a row does not free its space —
it marks the row dead and leaves autovacuum to reclaim it — so a bulk delete
produces a long vacuum on the busiest table in the system, a write-ahead log record
per row, and index bloat that outlives the vacuum. Run it daily and the vacuum never
catches up.

So `delivery_attempts` is partitioned by day, and retention is `DROP TABLE`. That
unlinks files: O(1), no vacuum, almost no WAL.

| What | Default | Env | How |
| --- | --- | --- | --- |
| Attempt log | 30 days | `RELAY_ATTEMPT_RETENTION_DAYS` | Partition drop |
| Succeeded deliveries | 30 days | `RELAY_SUCCEEDED_RETENTION_DAYS` | Batched delete |
| Dead letters | 90 days | `RELAY_DEAD_RETENTION_DAYS` | Batched delete |
| Idempotency keys | 24 hours | `RELAY_IDEMPOTENCY_RETENTION_SECS` | Batched delete |

Four windows, because the four are kept for different reasons and one number would
have to satisfy the longest. A dead letter is a webhook somebody is still owed, and
the point of the queue is that it can be replayed once the underlying problem is
fixed — so it outlives the successes by design. A `pending` delivery is never
deleted at any age: it is still owed.

Row deletes are batched and looped rather than issued as one large statement. A
single delete covering a month of history holds a transaction and a pile of row
locks on the table the claim query reads, and a delivery waiting behind a retention
sweep is a webhook arriving late for a reason no customer could ever be told.

### The default partition

There is one, it is meant to stay empty, and it exists because the alternative is
worse. Without it, an insert whose day has no partition *fails* — and that insert is
the same transaction that records a delivery's outcome, so the delivery would be
retried and the endpoint would receive it twice.

The trap is that once a row lands there, `CREATE TABLE ... PARTITION OF` for that day
fails permanently: Postgres refuses to create a partition covering rows the default
already holds. A naive maintainer can never catch up. So partitions are seeded by the
migration itself, created a fortnight ahead on every sweep, and the maintainer
recovers by detaching the default, creating the partition, moving the rows and
re-attaching — all in one transaction. `relay_attempt_default_partition_rows` is the
metric to alert on rather than graph.

## Reading the logs

Both binaries emit JSON when their stderr is a pipe and human-readable text when it
is a terminal. Nobody has to remember a flag: a container's stderr goes to a log
collector that wants JSON, a developer's goes to a terminal where JSON is
unreadable, and getting it wrong by default means either production logs that cannot
be queried or a local run that cannot be read. `RELAY_LOG_FORMAT=json|text` forces
it when the guess is wrong.

Every delivery opens one span carrying its id, its endpoint, its event type and its
attempt number, with four stages inside it — `gate`, `send`, `persist`, and the
`batch` claim that produced it. The span records how the delivery ended, so "what
happened to this one" is a single line rather than a join across several. Spans
close with the time they were busy, which is what turns *the delivery took nine
seconds* into *eight of them were inside `send`*. The noisy stages are `debug_span!`
and cost nothing at the default level.

The worker pool attaches each delivery's span to the spawned task explicitly. This
is the part that does not happen on its own: a span is ambient to the current
thread, `spawn` hands the future to whichever thread is free, and a task spawned
without it is silently orphaned — its events still appear, with no delivery id and
no parent, and nothing afterwards can say which delivery they described.

Signing secrets cannot reach the logs. `Debug` is written by hand for every row that
carries one and prints `<redacted>`, so a `?pending` added in a hurry during an
incident cannot leak one — and a field added to those rows later is redacted by
default rather than exposed by default.

## Running more than one dispatcher

Relay polls Postgres by default, and one node doing that is a complete system. When
one node is not enough, a Redis Streams broker carries "this delivery is ready" to a
fleet — set `RELAY_BROKER_URL` and the dispatcher switches modes.

Two rules make it safe.

**The broker is a transport, never the record.** Every message is a row id for a row
already committed in Postgres. No payload, no headers, no signing secret reaches
Redis. Everything it holds can be rebuilt from the `deliveries` table, and a
reconciliation sweep is the code that does the rebuilding — flushing Redis entirely,
three times, mid-run, loses zero deliveries.

**A message is not ownership.** Receiving one means "this delivery is worth trying",
not "you exclusively own it". Redis redelivers, and reclaim deliberately hands the
same message to a second consumer. What actually prevents a double send is the
database lease that was already there: a consumer claims the row before it sends, and
a claim that loses acknowledges the message and moves on.

The sweep has one non-obvious rule: it stands down while the broker still holds
unread entries. A row that was announced and has not moved looks the same whether its
message was lost or is queued behind a backlog, and re-announcing the second case
appends to the very backlog that made it look stalled. That feedback loop was
measured before the guard existed.

See [docs/deployment.md](docs/deployment.md).

## Health and readiness

`GET /healthz` asks whether the process is running and nothing else. `GET /readyz`
asks whether this instance should be sent traffic, and answers `503` with a body
naming the failure when it should not.

They are separate because an orchestrator restarts what fails liveness. A liveness
probe that also checked the database would turn one database blip into every replica
restarting at once.

Readiness runs three checks: the database answers, the dispatcher has reported
recently, and the queue is draining. The third is the interesting one. It measures
*lateness* — how far past its due time the oldest pending delivery is — rather than
depth, because depth lies in both directions: large and harmless while a burst
drains, three rows and catastrophic when those three have been stuck for an hour.
Every deliberate wait in Relay moves a row's due time forward, so a delivery is late
only when nothing came to collect it.

The heartbeat and the lateness check catch different failures, which is why both are
there. A dispatcher that died overnight with an empty queue makes nothing late. A
dispatcher wedged on a poisoned row goes on beating happily.

## How fast it actually is

5,000 deliveries a second sustained on a laptop with a p99 of 106 ms, and 6,781 a
second draining a backlog, with zero lost across every run. The requirement was
1,000.

The bottleneck is not the worker pool. Sixteen times the workers buys 4% over
sixteen of them, because a worker spends its life waiting on a socket. What matters
is round trips to the database: claiming one row at a time costs two-thirds of the
throughput, and raising the connection pool from 8 to 32 is worth more than
quadrupling the workers.

Method, the full configuration sweep, and what to change first are in
[docs/load-test.md](docs/load-test.md).

## License

MIT
