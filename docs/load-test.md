# Load test

Measured, not estimated. Every number here came from a run on the machine described
below; nothing is extrapolated.

## The headline

| Criterion | Required | Measured |
|---|---|---|
| Sustained deliveries per second | 1,000 | **5,000** sustained, **6,781** draining a backlog |
| p99 latency, excluding receiver time | < 5s | **62 ms** at 1,000/s, **106 ms** at 5,000/s |
| Lost deliveries | 0 | **0** of 265,484 across every run |

Measured against the containerised dispatcher — the artifact that actually ships,
not a pool assembled inside the harness.

**Machine:** MacBook, 8 cores, arm64, macOS 14.2. Docker Desktop with 8 CPUs and
4 GB. Postgres 17-alpine in a container, `synchronous_commit=on`, on a Docker volume.
A server would do considerably better; the point is the shape, not the ceiling.

## How it is measured

```bash
# The whole stack, then a paced producer against it
docker compose up -d
DATABASE_URL=postgres://relay:relay@localhost:5433/relay_load \
  LOADTEST_MODE=feed LOADTEST_RATE=1000 LOADTEST_SECONDS=30 \
  cargo run --release -p relay-loadtest
DATABASE_URL=... LOADTEST_MODE=report cargo run --release -p relay-loadtest
```

Modes: `feed` (paced production against a dispatcher you started), `seed` (fill a
backlog), `report` (read the result), `drain` (fill and empty in-process), `sweep`
(walk a configuration grid), and the default, one sustained in-process run.

Three decisions shape every number:

**Sustained, not drained.** Seeding a hundred thousand rows and timing the clearout
measures peak drain rate against a queue that is never empty — the most flattering
framing available — and it makes latency meaningless, because the last row waits for
the entire run. p99 then reports how much was seeded rather than anything about the
system. So the producer runs at a fixed rate *while* the dispatcher works, and the
question is whether the dispatcher keeps up.

**Latency excludes the receiver.** `(attempt.at - delivery.created_at) -
attempt.latency_ms`: the time a delivery spent inside Relay, waiting to be claimed,
gated and persisted. A slow receiver is not a Relay latency problem, and leaving it
in would let a fast receiver flatter the result.

**Loss is counted from the attempt log, not from the sender.** A counter in the
sender proves only that the sender believes it sent something. The attempt log is
what a customer would be shown, and it is written in the transaction that resolves
the delivery.

The receiver answers on `/sink`, which returns `200` and records nothing but a count.
Every other route in the testkit appends the body, path and signature to vectors
behind one mutex — right for a test asserting on what arrived, wrong at thousands of
requests a second, where that lock becomes the narrowest thing in the system and the
run measures the laboratory instead of Relay.

## Containerised results

Producer on the host, dispatcher and receiver in containers. 8 endpoints,
64 workers, batch 32, 32 connections.

| Offered | Delivered | Lost | p50 | p95 | p99 | max |
|---|---|---|---|---|---|---|
| 1,000/s × 30s | 30,000 | 0 | 11 ms | 27 ms | 62 ms | 221 ms |
| 3,000/s × 20s | 60,000 | 0 | 13 ms | 43 ms | 70 ms | 150 ms |
| 5,000/s × 20s | 100,000 | 0 | 13 ms | 43 ms | 106 ms | 180 ms |
| 9,000/s × 20s | 95,484 | 0 | 13 ms | 37 ms | 59 ms | 211 ms |

The last row is the interesting one. Asked for 9,000/s the producer managed 4,774/s —
**the load generator saturates before Relay does.** Latency did not move, so this
measures the harness, not the system. The honest ceiling comes from draining a
pre-built backlog with nothing else writing: **40,000 deliveries in 5.9 s, 6,781/s,
zero lost.**

## Where the limit actually is

The issue named two suspects: the claim query and the connection pool. Both matter,
in sequence, and the worker pool — the thing most people would tune first — is not
the bottleneck at all.

**Workers stop helping after 16.** In-process, 3,000/s offered:

| Workers | 8 | 16 | 32 | 64 | 128 |
|---|---|---|---|---|---|
| Delivered/s | 949 | 1,778 | 1,844 | 1,893 | 1,847 |

Sixteen times the workers buys 4% over 16. A worker spends its life waiting on a
socket, so more of them adds queueing, not capacity.

**The claim query dominates at small batch sizes, then stops mattering.** Draining a
backlog, 64 workers:

| Batch | 1 | 8 | 32 | 256 |
|---|---|---|---|---|
| Delivered/s | 1,176 | 2,469 | 3,467 | 3,606 |

Claiming one row per round trip costs two-thirds of the throughput. Amortising it
across 32 recovers nearly all of that, and 256 adds a further 4%. **Batch size is the
single highest-leverage setting, and 32 is already most of the way to the plateau.**

**Past that it is round trips, which is why connections matter.** In-process,
3,000/s offered, 32 workers:

| Connections | 2 | 4 | 8 | 16 | 32 |
|---|---|---|---|---|---|
| Delivered/s | 1,089 | 1,455 | 1,844 | 1,713 | 1,920 |

Every delivery ends in its own write transaction, so the pool bounds how many can be
resolved at once. Doubling from the default 8 to 32 is worth more than quadrupling
the workers.

**It is not disk.** Running with `synchronous_commit=off` — trading durability for
speed, purely as a diagnostic — moved throughput from 1,824/s to 2,026/s. An 11%
gain rules out WAL fsync as the dominant cost and points at the round trips
themselves.

**And the producer competes.** In-process, sustained load tops out near 2,100/s;
draining the same queue with no producer running reaches 3,600/s. The producer is a
write workload of its own against the same database, and roughly 40% of the sustained
figure was contention with it. Blaming the dispatcher for that would have been
exactly the assumed bottleneck the issue warns against.

## What to change first

In order of measured leverage:

1. **`RELAY_BATCH_SIZE`** — the default of 32 is right. Below 16 it hurts badly.
2. **`RELAY_DB_CONNECTIONS`** — raise from 8 to 32 under sustained load. Worth more
   than any change to the worker count.
3. **`RELAY_WORKERS`** — 32 is ample. Raising it is not the lever it looks like.

Beyond that, the next real gain is architectural rather than a setting: batching the
outcome writes the way the claim is already batched. Every delivery currently costs
one write transaction, and that is what the numbers above run into. That is a change
worth making only with a measurement to justify it — which is now what this document
is for.

## Caveats

- One machine, macOS, Docker Desktop. Container networking on macOS goes through a
  VM, which taxes every round trip; a Linux host would shift the numbers.
- The producer writes through the same Postgres the dispatcher reads. Real ingest
  arrives over HTTP into the API, which is a different cost.
- All endpoints answer instantly. Real receivers take tens of milliseconds, which
  changes what the worker count is for — workers exist to wait, and a slow receiver
  is exactly when raising them helps.
- No failures, retries or breaker activity. This measures the happy path.
