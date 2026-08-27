# Running Relay

One command, from a clean machine to a working stack.

```bash
docker compose up --build
```

That builds three images and starts six containers. First build takes a few minutes
because it compiles the workspace in release mode; after that it is cached.

| Service      | Port   | What it is                                              |
|--------------|--------|---------------------------------------------------------|
| `api`        | `8080` | Ingest and admin HTTP surface                            |
| `dispatcher` | `9091` | The send loop. Serves metrics only; makes no other port  |
| `postgres`   | `5433` | The queue and everything else. `5433` to stay off a local Postgres |
| `receiver`   | `9099` | A stand-in customer endpoint that verifies signatures    |
| `prometheus` | `9090` | Scrapes both Relay processes                             |
| `grafana`    | `3000` | The dashboard, no login, opens on it directly            |

Check it came up:

```bash
curl -s localhost:8080/readyz | jq
```

## Delivering your first webhook

This is the whole loop: register a destination, send an event, watch it arrive.

**1. Register an endpoint.** The `receiver` container is reachable from the
dispatcher by its service name.

```bash
curl -sX POST localhost:8080/v1/endpoints \
  -H 'content-type: application/json' \
  -d '{"url":"http://receiver:9099/verify","event_types":["order.paid"]}'
```

```json
{
  "id": "49220fbc-...",
  "url": "http://receiver:9099/verify",
  "transport": "http",
  "secret": "whsec_cbeda851...",
  "rate_per_second": 10.0,
  "burst": 20.0
}
```

Keep the `secret`. It is returned exactly once — Relay stores it but will never show
it to you again, because a secret that can be re-read is a secret that leaks through
whatever can read it.

**2. Tell the receiver that secret.** A real receiver reads it from its own
configuration. This one has to be told, because the secret did not exist until step
one:

```bash
curl -sX POST localhost:9099/secret --data 'whsec_cbeda851...'
```

**3. Send an event.** The event type goes in a header, or in a `type` field in the
body:

```bash
curl -sX POST localhost:8080/v1/events \
  -H 'content-type: application/json' \
  -H 'relay-event-type: order.paid' \
  -d '{"order":"A-1","amount":4999}'
```

```json
{"event_id":"296e21d4-...","delivery_ids":["c252efa2-..."]}
```

`202` means stored and queued, not delivered. Relay never makes you wait on somebody
else's server.

**4. Confirm it arrived**, both from the receiver and from Relay's own record:

```bash
curl -s localhost:9099/received | jq        # delivery ids the receiver saw
curl -s localhost:8080/v1/deliveries/c252efa2-... | jq
```

The second one is the authority. It carries every attempt, with the status code, the
latency, the response snippet and the reason for any retry.

## Integrating a real receiver

Everything you need to write one.

**Relay sends** a `POST` with the exact bytes you handed it — never re-encoded, so a
signature computed over the body always matches — and these headers:

| Header             | Meaning                                              |
|--------------------|------------------------------------------------------|
| `relay-delivery-id`| Stable across retries of the same delivery           |
| `relay-event-type` | What happened                                        |
| `relay-timestamp`  | Unix seconds, and part of what is signed             |
| `relay-signature`  | `v1=<hex>`, comma-separated when more than one is live |

**Verify like this**, in order:

1. Reject if `relay-timestamp` is more than five minutes from now. Without this a
   captured request can be replayed forever. The timestamp is inside the signed
   string, so it cannot be edited to suit.
2. Compute `HMAC-SHA256(secret, "<timestamp>.<raw body>")`, hex-encoded.
3. Compare against **each** entry in `relay-signature` with a constant-time
   comparison, and accept if any matches. More than one entry means a rotation is in
   progress and both secrets are valid — an endpoint that only reads the first will
   start failing halfway through the next one.

Compare over the **raw bytes** you received. Parsing the JSON and re-serialising it
changes key order and whitespace, and the signature will not match.

**Answer quickly.** Relay gives you 10 seconds in total. Do the work afterwards:
write the body down, return `2xx`, process later.

**Answer honestly.** `2xx` is success. `4xx` other than `408`/`429` is permanent and
Relay stops immediately — do not return `400` for a problem on your side, or you
throw the event away. `5xx`, `408` and `429` are retried with exponential backoff,
and `429` with a `Retry-After` is obeyed exactly.

**Expect duplicates.** Delivery is at-least-once. A network failure after you commit
but before your `200` arrives is indistinguishable from a failure before it, so Relay
retries and you see the event twice. `relay-delivery-id` is stable across those
retries: store it and ignore ids you have already handled.

A working implementation to copy is `crates/testkit/src/lib.rs`, which is written
independently of the signer precisely so it can disagree with it.

## Health and readiness

Two endpoints answering two different questions. Conflating them causes a specific
outage: an orchestrator restarts what fails liveness, so a liveness probe that also
checks the database turns one database blip into every replica restarting at once.

**`GET /healthz`** — is this process running? Nothing shared. This is what a restart
policy should watch.

**`GET /readyz`** — should this instance be sent traffic? Three checks, `200` if all
pass and `503` with a body naming the failure if not:

```json
{
  "ready": true,
  "database": {"status": "pass", "detail": "reachable"},
  "dispatcher": {"status": "pass", "detail": "last reported 2.0s ago"},
  "queue": {"status": "pass", "detail": "nothing pending"}
}
```

- **database** — a round trip. Nothing works without it, and if it fails the other
  two report `skipped` rather than inventing an answer.
- **dispatcher** — the dispatcher writes a heartbeat every few seconds. This catches
  the case the queue check cannot see: a dispatcher that died overnight looks exactly
  like one with nothing to do.
- **queue** — how far past its due time the oldest pending delivery is. This is the
  one that matters, and it is *lateness*, not depth. Depth lies in both directions:
  large and healthy while a burst drains, three rows and catastrophic when those
  three have been stuck for an hour. Every deliberate wait in Relay — a backoff, a
  rate limit, an open breaker, a concurrency cap — moves a row's due time forward, so
  a delivery is late only when nothing came to collect it.

A dispatcher that is alive but wedged fails the third check while passing the second.
That combination is the whole point of having both.

## Running more than one dispatcher

Relay polls Postgres by default, and one node doing that is a complete system. When
one node is not enough there is a second mode: a Redis Streams broker carries "this
delivery is ready" to a fleet of dispatchers.

```bash
docker compose up -d redis
RELAY_BROKER_URL=redis://redis:6379 docker compose up -d dispatcher
```

Three things are worth knowing before turning it on.

**The broker is a transport, never the record.** Every message is a row id for a row
that is already committed in Postgres. No payload, no headers, no signing secret ever
reaches Redis — losing it, or someone reading it, exposes a list of UUIDs. Everything
it holds can be rebuilt by reading the `deliveries` table.

**A message is not ownership.** Receiving one means "this delivery is worth trying",
not "you exclusively own it". Redis redelivers, and reclaim deliberately hands the
same message to a second consumer when the first goes quiet. The thing that actually
prevents two sends is the database lease that was already there: a consumer claims
the row before it sends, and a claim that loses acknowledges the message and moves
on. At-least-once delivery plus "send immediately" is at-least-twice.

**Redis Streams rather than Kafka.** Consumer groups, acknowledgements and
idle-message reclaim in one container that starts in a second. Kafka's log semantics
buy nothing until several independent systems want the same stream, and the
operational footprint is disproportionate long before then. The `Broker` trait is
what keeps that reversible.

| Variable | Default | |
|---|---|---|
| `RELAY_BROKER_URL` | unset | Unset or empty means polling. Setting it switches on the broker |
| `RELAY_BROKER_STREAM` | `relay:deliveries` | |
| `RELAY_BROKER_GROUP` | `relay-dispatchers` | Every dispatcher joins the same one, which is what makes them split the work rather than each receiving everything |
| `RELAY_CONSUMER_NAME` | random per process | Must be stable per process and distinct between them: Redis tracks unacknowledged messages per consumer name |
| `RELAY_BROKER_BLOCK_MS` | `500` | How long a read waits for a message |
| `RELAY_BROKER_RECLAIM_IDLE_SECS` | `60` | Before another consumer may take over a held message. Keep above the request timeout, or reclaim takes work from consumers that are merely slow |
| `RELAY_BROKER_RECLAIM_EVERY_SECS` | `15` | |
| `RELAY_OUTBOX_BATCH` | `256` | Rows announced per pass, and the most a crash between marking and publishing can strand |
| `RELAY_OUTBOX_IDLE_MS` | `100` | |
| `RELAY_OUTBOX_STALE_SECS` | `60` | How long a row may sit announced before the sweep assumes its message is gone |
| `RELAY_OUTBOX_SWEEP_SECS` | `30` | |
| `RELAY_OUTBOX_SWEEP_BELOW_UNREAD` | `256` | The sweep stands down above this many unread entries. See below |

### Losing the broker

Every message is a row id for a row already committed in Postgres, so nothing the
broker holds is irreplaceable. A reconciliation sweep finds rows that were announced
and never moved, and announces them again. Flushing Redis entirely, three times,
mid-run, loses zero deliveries — that is a test, not a claim.

The sweep has one rule that is not obvious. **It stands down while the broker still
holds unread entries.** A row that is marked as announced and has not moved looks
identical whether its message was lost or is merely queued behind a long backlog, and
sweeping the second case appends another entry to the very backlog that made it look
stalled — which makes the next sweep find more rows, which appends more entries.

That is a positive feedback loop, and it was measured before the guard existed: a
chaos run of 30,000 deliveries produced 119,000 published messages and a stream of
70,000 entries, with consumers spending their time acknowledging duplicates of work
that had already succeeded. With the guard, the same run produces 51,000 published
messages, 21,000 stream entries, and 64 stale messages instead of 4,768.

`relay_outbox_requeued_total` should sit at zero. Anything else means messages are
going missing between the publisher and the consumers, and it is the only thing that
would say so.

## Configuration

Everything is an environment variable, and every one has a working default. Compose
passes them through as `${VAR:-default}`, so they can be set in the shell or in a
`.env` file next to `docker-compose.yml` without editing anything.

### Both processes

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | `postgres://relay:relay@postgres:5432/relay` | |
| `RELAY_LOG_FORMAT` | auto | `json` or `text`. Guessed from whether stderr is a terminal |
| `RUST_LOG` | `info` | Standard `tracing` filter |
| `RELAY_ALLOW_PRIVATE_ENDPOINTS` | `false` | **See the warning below.** Compose sets `true` |
| `RELAY_REQUIRE_HTTPS` | opposite of the above | |
| `RELAY_ALLOWED_PORTS` | `443` when strict, any when private is allowed | Comma-separated |
| `RELAY_MAX_PAYLOAD_BYTES` | `262144` | Must match across both, or ingest accepts what delivery cannot send |

### API

| Variable | Default | |
|---|---|---|
| `RELAY_API_BIND` | `0.0.0.0:8080` | |
| `RELAY_SECRET_OVERLAP_SECS` | `86400` | How long both signatures are sent after a rotation |
| `RELAY_HEARTBEAT_MAX_AGE_SECS` | `20` | Several dispatcher beats, not one |
| `RELAY_MAX_LATENESS_SECS` | `60` | Above the worst honest lateness, or a busy Relay calls itself unready |

### Dispatcher

| Variable | Default | |
|---|---|---|
| `RELAY_METRICS_BIND` | `0.0.0.0:9091` | |
| `RELAY_WORKERS` | `32` | Concurrent deliveries |
| `RELAY_BATCH_SIZE` | `32` | Rows claimed per pass |
| `RELAY_DB_CONNECTIONS` | `8` | Fewer than workers: a connection is held for the claim and the write, never across the request |
| `RELAY_IDLE_POLL_MS` | `250` | Wait when the queue is empty |
| `RELAY_HEARTBEAT_INTERVAL_SECS` | `5` | Keep well under `RELAY_HEARTBEAT_MAX_AGE_SECS` |
| `RELAY_SHUTDOWN_DEADLINE_SECS` | `15` | Drain time before in-flight work is abandoned to the reaper |
| `RELAY_LEASE_TTL_SECS` | `30` | **Must exceed `RELAY_REQUEST_TIMEOUT_SECS`.** Checked at startup: a lease expiring mid-request lets a second worker send the same delivery |
| `RELAY_REAP_INTERVAL_SECS` | `10` | |
| `RELAY_CONNECT_TIMEOUT_SECS` | `5` | |
| `RELAY_READ_TIMEOUT_SECS` | `5` | Resets per byte, which is why the next one exists |
| `RELAY_REQUEST_TIMEOUT_SECS` | `10` | Total. The only bound a trickling response cannot defeat |
| `RELAY_MAX_RESPONSE_BYTES` | `2048` | How much of a response is read before the rest is dropped |
| `RELAY_MAX_IN_FLIGHT` | `64` | Global cap, protects Relay's own sockets |
| `RELAY_MAX_PER_ENDPOINT` | `8` | Bulkhead, stops one dead endpoint absorbing the pool |
| `RELAY_RATE_LIMIT` | `true` | `false` only for a deliberate load test |
| `RELAY_BREAKER` | `true` | `false` only for a deliberate load test |
| `RELAY_BREAKER_THRESHOLD` | `5` | Consecutive failures before an endpoint is cut off |
| `RELAY_BREAKER_COOLDOWN_SECS` | `30` | |
| `RELAY_BREAKER_MAX_COOLDOWN_SECS` | `300` | |
| `RELAY_IDEMPOTENCY_RETENTION_SECS` | `86400` | How long a key is honoured. Shorter widens the window in which a retry creates a second event |
| `RELAY_ATTEMPT_RETENTION_DAYS` | `30` | Enforced by dropping whole partitions |
| `RELAY_SUCCEEDED_RETENTION_DAYS` | `30` | Not shorter than the above: deleting a delivery cascades into the attempt log |
| `RELAY_DEAD_RETENTION_DAYS` | `90` | Longest. A dead letter is a webhook somebody is still owed |
| `RELAY_RETENTION_BATCH` | `5000` | Rows per delete before the sweep yields |
| `RELAY_PARTITION_DAYS_AHEAD` | `14` | Attempt partitions created in advance |
| `RELAY_PRUNE_INTERVAL_SECS` | `3600` | |

### Receiver

| Variable | Default | |
|---|---|---|
| `RELAY_TESTKIT_BIND` | `0.0.0.0:9099` | |
| `RELAY_TESTKIT_SECRET` | `whsec_test` | Overridable at runtime with `POST /secret` |

## Before this goes anywhere real

This compose file is for a laptop. Three things about it are indefensible elsewhere,
and each is deliberate:

**`RELAY_ALLOW_PRIVATE_ENDPOINTS=true`.** The receiver lives on a private Docker
address, so the stack would refuse every delivery without it. It is also the single
most dangerous setting Relay has: it lets a customer register an endpoint pointing at
anything reachable from the container, including a cloud provider's metadata service,
which answers without authentication to whatever asks. Relay makes outbound requests
to customer-controlled addresses for a living — that is a server-side request forgery
engine unless this stays off.

**The admin API has no authentication.** Anyone who can reach `:8080` can register an
endpoint, read the dead letter queue and rotate a secret. There is no auth milestone
yet, which is the reason this is a local stack and not a deployment.

**Grafana has no login,** and Postgres uses `relay:relay`.

The parts that *are* production shape: both processes run as an unprivileged user,
the dispatcher handles `SIGTERM` and drains in-flight deliveries before exiting, both
binaries are PID 1 so they receive that signal at all, migrations are embedded in the
binary and run under an advisory lock so two processes starting together cannot race,
and readiness is honest enough to fail while the process is still perfectly alive.

## Developing against the stack

Compiling inside Docker on every edit is the slow loop. Run Postgres in a container
and the code on the host:

```bash
docker compose up -d postgres
export DATABASE_URL=postgres://relay:relay@localhost:5433/relay
cargo test
```

Prometheus scrapes Relay through the host address, which is correct either way:
compose publishes `8080` and `9091` to the host, so that address reaches the
container when the stack is up and the host process when it is not. Only one can own
each port, so there is never any question which is being measured.

Do not try to run both at once. Beyond the port collision, the queue gauges describe
rows in a shared database, and two dispatchers reporting them would double every
panel that sums across instances.
