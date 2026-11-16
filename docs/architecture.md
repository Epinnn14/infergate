# infergate internals

## Components

| Component | Language | State | Scales by |
|---|---|---|---|
| gateway | Go, standard library only | none; caches the registry snapshot and live-worker list for 250 ms | HPA on CPU |
| worker | Python asyncio | prefix cache (in GPU memory for real engines) | KEDA on shared-stream lag, per model version |
| registry | Python FastAPI | PostgreSQL: models, versions, weights, API key hashes, revisions | replicas; gateways poll it, so it is not on the request path |
| Redis | - | queues, reply streams, heartbeats, rate-limit buckets | one per cell; streams shard by version on Redis Cluster |

The registry being down doesn't stop serving: gateways keep the last good snapshot.

## Redis layout

Go side: `internal/queue/queue.go`. Python side: `worker/infergate_worker/protocol.py`.

| Key | Type | Written by | Purpose |
|---|---|---|---|
| `ig:q:<version>` | stream, group `workers` | gateway (overflow), workers (hand-back, rescue) | shared queue of a model version; KEDA scales on its lag |
| `ig:w:<worker>` | stream, group `workers` | gateway (affinity) | requests routed to one worker for its prefix cache |
| `ig:live:<version>` | sorted set worker → last heartbeat ms | workers | who can take traffic |
| `ig:draining:<version>` | sorted set worker → last heartbeat ms | workers shutting down | not routed to, but still watched |
| `ig:worker:<worker>` | hash | workers | in-flight count, capacity, cache stats |
| `ig:r:<request>` | stream | worker | events `token`, `done`, `error`, `restart`; expires 120 s after the end |
| `ig:cancel:<request>` | string, TTL 120 s | gateway | client went away; stop generating |
| `ig:rl:<key>:<rpm\|tpm>` | hash | gateway (Lua) | token bucket |
| `ig:rescue:<worker>` | string NX, TTL 30 s | workers | only one peer rescues a dead worker |

A request message carries the id, version, messages (JSON), `max_tokens`, temperature, prefix
hash, reply stream, absolute deadline, enqueue time and attempt number.

## Routing

```
prefix  = sha256(all messages except the last)[:16]        # "" for a first message
ranking = sort live workers by mix(fnv64a(prefix + worker))  # rendezvous hashing
load(w) = (inflight + unread in ig:w:<w>) / capacity

if prefix and ranking[0].load < max_load:  → ig:w:ranking[0]   (affinity)
elif prefix and ranking[1].load < max_load: → ig:w:ranking[1]   (spill)
else:                                       → ig:q:<version>    (shared)
```

`mix` is the murmur3 64-bit finalizer. Plain FNV-1a over short, similar strings spread
conversations unevenly across workers, and the router tests check the spread.

Version choice comes first: `weight`-proportional, keyed by hash(API key, first user message),
so a conversation keeps its version while weights stay the same.

## Delivery and failure handling

Requests are delivered at least once. Each redelivery increments `attempt`, and a worker that
starts an attempt above 1 first emits `restart`. The gateway then drops what it had buffered for a
non-streaming request. A streaming request that already sent tokens ends with an error chunk
instead of sending text twice.

| Failure | Detection | Handling |
|---|---|---|
| Client disconnects | request context done | `SET ig:cancel:<id>`; worker stops within 8 tokens; status 499 |
| Deadline passes | gateway timer; worker checks before starting | 504; worker skips expired work |
| Worker crashes or is OOM-killed | no heartbeat for `dead_after` (15 s default) | a peer takes `ig:rescue:<w>`, `XAUTOCLAIM`s the pending messages (attempt+1), moves unread ones to the shared stream, then deletes the stream and hash |
| Worker hangs with a shared message | message idle longer than `reclaim_idle` | `XAUTOCLAIM` on the shared stream by any worker, attempt+1 |
| Graceful shutdown (SIGTERM) | - | move from live to draining set → finish in-flight work within `shutdown_grace` → move unread messages to the shared stream → leave |
| Killed during that shutdown | draining heartbeat stops | same rescue as a crash |
| Gateway replica dies | client sees a dropped connection | Kubernetes reschedules; the worker sees no reader and the reply stream expires |
| Overload | shared-stream depth ≥ `max_queue` | 503 before enqueueing; KEDA is already scaling on the lag |
| Registry down | poll fails | gateways serve with the last snapshot |
| Version with 0 replicas gets traffic | lag on its shared stream | KEDA activation (lag ≥ 1) scales it from zero; requests wait within their deadline |

## Rate limiting

There is one token bucket per (key, limit). The Lua script refills it by elapsed time, takes
`cost` if available and otherwise returns the wait until it would be. RPM costs 1. TPM costs
`prompt_estimate + max_tokens` up front, and the difference from actual usage is refunded once
the request finishes, so long `max_tokens` values don't starve a key. The limit is enforced
across every gateway replica, with one round trip per request.

## Autoscaling signals

| What | Signal | Why |
|---|---|---|
| Workers (per version) | KEDA `redis-streams` trigger, `lagCount` on `ig:q:<version>` | only overflow reaches the shared stream, so lag means the current replicas are full |
| Gateways | HPA on CPU | the gateway is CPU-bound (JSON, SSE framing) and stateless |
| Scale-down | 120 s stabilization, 1 pod / 30 s | workers drain gracefully, and slow scale-down keeps prefix caches warm |

## Fake backend model

The step time of one decode iteration is `1 / tokens_per_s × (1 + batch_penalty × (batch − 1))`.
Prefill sleeps `prompt_tokens × prefill_ms_per_token`, or 15% of that when the prefix hash is in
the worker's LRU cache of `IG_FAKE_PREFIX_CACHE` entries. Output length is 48–128 tokens, picked
from the request id hash and capped by `max_tokens`. This is the cost structure of a real engine,
in simplified form: a large batch amortizes the per-step cost, and a warm KV prefix skips prefill.
