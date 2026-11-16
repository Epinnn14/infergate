"""The worker loop: take requests from Redis Streams, generate, stream tokens back.

Reliability, in order of what can go wrong:

* **A worker dies mid-request.** Requests are read with XREADGROUP, so they stay pending
  until acknowledged. Other workers reclaim pending shared-queue messages idle for longer
  than ``reclaim_idle_s`` (XAUTOCLAIM), and take over a dead worker's own stream once its
  heartbeat is ``dead_after_s`` old. Delivery is at-least-once; the gateway reads the reply
  stream, so a re-run request still answers the client if it is still waiting.
* **A worker is shut down (rolling update, scale-in).** On SIGTERM it leaves the live set
  (the gateway stops routing to it), finishes in-flight requests within the grace period,
  and moves anything still waiting in its own stream to the shared queue.
* **The client goes away.** The gateway sets a cancel key; generation stops early.
* **A request waited too long.** Past its deadline it is answered with an error, not run.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

import redis.asyncio as aioredis
from prometheus_client import Counter, Gauge, Histogram
from redis.exceptions import ResponseError

from . import protocol as p
from .backends import Backend, Job

log = logging.getLogger("infergate.worker")

REQUESTS = Counter("infergate_worker_requests_total", "Requests finished, by outcome.", ["model", "outcome"])
TOKENS = Counter("infergate_worker_tokens_total", "Tokens generated.", ["model"])
CACHE = Counter("infergate_worker_prefix_cache_total", "Prefix cache lookups.", ["model", "result"])
INFLIGHT = Gauge("infergate_worker_inflight", "Requests being generated.", ["model"])
QUEUE_WAIT = Histogram(
    "infergate_worker_queue_wait_seconds", "Time from enqueue to start.", ["model"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
TTFT = Histogram(
    "infergate_worker_ttft_seconds", "Start to first token.", ["model"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
RECLAIMED = Counter("infergate_worker_reclaimed_total", "Requests taken over from other workers.", ["model", "how"])


class Worker:
    def __init__(self, cfg, backend: Backend, rds: aioredis.Redis | None = None):
        self.cfg = cfg
        self.backend = backend
        self.r = rds or aioredis.from_url(cfg.redis_url, decode_responses=True)
        self.id = cfg.worker_id
        self.own = p.worker_stream(self.id)
        self.shared = p.shared_stream(cfg.model)
        self.tasks: set[asyncio.Task] = set()
        self.stopping = asyncio.Event()
        self.drained = False
        self.served = 0
        self.started = time.time()

    # ------------------------------------------------------------ setup

    async def _ensure_group(self, stream: str) -> None:
        try:
            await self.r.xgroup_create(stream, p.GROUP, id="0", mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def heartbeat(self) -> None:
        if self.drained:
            return
        await self.r.hset(
            p.worker_hash(self.id),
            mapping={
                "model": self.cfg.model,
                "inflight": len(self.tasks),
                "capacity": self.cfg.capacity,
                "started": int(self.started),
                "served": self.served,
                **{k: v for k, v in self.backend.stats().items() if isinstance(v, (int, float, str))},
            },
        )
        await self.r.expire(p.worker_hash(self.id), int(self.cfg.dead_after_s * 4))
        # while draining, keep beating in the draining set: gateways stop routing here, but if
        # the pod is killed before the drain ends, peers still notice and rescue its requests
        beat = p.draining_set(self.cfg.model) if self.stopping.is_set() else p.live_set(self.cfg.model)
        await self.r.zadd(beat, {self.id: int(time.time() * 1000)})

    async def _heartbeat_loop(self) -> None:
        while True:
            try:
                await self.heartbeat()
            except Exception as e:  # keep beating through Redis hiccups
                log.warning("heartbeat failed: %s", e)
            await asyncio.sleep(self.cfg.heartbeat_s)

    # ------------------------------------------------------------ serving one request

    async def handle(self, stream: str, msg_id: str, fields: dict) -> None:
        INFLIGHT.labels(self.cfg.model).inc()
        # the gateway reads this to judge load; don't wait for the next heartbeat
        await self.r.hincrby(p.worker_hash(self.id), "inflight", 1)
        try:
            await self._handle(stream, msg_id, fields)
        finally:
            INFLIGHT.labels(self.cfg.model).dec()
            await self.r.hset(p.worker_hash(self.id), "inflight", max(0, len(self.tasks) - 1))

    async def _handle(self, stream: str, msg_id: str, fields: dict) -> None:
        rid = fields.get("id", "")
        reply = fields.get("reply") or f"ig:r:{rid}"
        model = self.cfg.model

        async def finish(outcome: str) -> None:
            await self.r.expire(reply, 120)
            await self.r.xack(stream, p.GROUP, msg_id)
            if stream != self.shared:
                await self.r.xdel(stream, msg_id)  # own streams stay short: XLEN = backlog
            REQUESTS.labels(model, outcome).inc()
            if outcome == "ok":
                self.served += 1

        try:
            deadline = int(fields.get("deadline_ms") or 0) / 1000
            enqueued = int(fields.get("enqueued_ms") or 0) / 1000
            if enqueued:
                QUEUE_WAIT.labels(model).observe(max(0.0, time.time() - enqueued))
            if deadline and time.time() > deadline:
                await self.r.xadd(reply, {"type": "error", "text": "deadline exceeded while queued", "worker": self.id})
                await finish("expired")
                return
            if await self.r.exists(p.cancel_key(rid)):
                await finish("cancelled")
                return
            job = Job(
                id=rid,
                model=fields.get("model", model),
                messages=json.loads(fields.get("messages") or "[]"),
                max_tokens=int(fields.get("max_tokens") or 256),
                temperature=float(fields.get("temperature") or 1.0),
                prefix=fields.get("prefix", ""),
            )
        except Exception as e:
            await self.r.xadd(reply, {"type": "error", "text": f"bad request: {e}", "worker": self.id})
            await finish("bad_request")
            return

        if int(fields.get("attempt") or 1) > 1:
            # another worker started this and died: the gateway drops what it got from that one
            await self.r.xadd(reply, {"type": "restart", "worker": self.id})

        start = time.time()
        first = True

        async def emit(text: str) -> None:
            nonlocal first
            if first:
                first = False
                TTFT.labels(model).observe(time.time() - start)
            await self.r.xadd(reply, {"type": "token", "text": text, "worker": self.id})

        async def cancelled() -> bool:
            return bool(await self.r.exists(p.cancel_key(rid)))

        try:
            res = await self.backend.generate(job, emit, cancelled)
        except Exception as e:
            log.exception("generation failed for %s", rid)
            await self.r.xadd(reply, {"type": "error", "text": f"generation failed: {e}", "worker": self.id})
            await finish("error")
            return
        TOKENS.labels(model).inc(res.output_tokens)
        if job.prefix:
            CACHE.labels(model, "hit" if res.cache_hit else "miss").inc()
        await self.r.xadd(
            reply,
            {
                "type": "done",
                "worker": self.id,
                "finish_reason": res.finish_reason,
                "prompt_tokens": res.prompt_tokens,
                "output_tokens": res.output_tokens,
                "cache_hit": int(res.cache_hit),
            },
        )
        await finish(res.finish_reason if res.finish_reason == "cancelled" else "ok")

    def _spawn(self, stream: str, msg_id: str, fields: dict) -> None:
        t = asyncio.create_task(self.handle(stream, msg_id, fields))
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)

    # ------------------------------------------------------------ main loops

    async def _consume(self) -> None:
        while not self.stopping.is_set():
            free = self.cfg.capacity - len(self.tasks)
            if free <= 0:
                await asyncio.sleep(0.005)
                continue
            try:
                # own stream first: requests routed here for the prefix cache
                res = await self.r.xreadgroup(
                    p.GROUP, self.id, {self.own: ">", self.shared: ">"}, count=free, block=500
                )
            except Exception as e:
                if self.stopping.is_set():
                    break
                log.warning("read failed: %s", e)
                await asyncio.sleep(0.5)
                continue
            for stream, entries in res or []:
                for msg_id, fields in entries:
                    self._spawn(stream, msg_id, fields)

    async def reclaim(self) -> int:
        """Take over stalled shared-queue messages and dead workers' own streams."""
        taken = 0
        free = max(0, self.cfg.capacity - len(self.tasks))
        if free:
            _, claimed, _ = await self.r.xautoclaim(
                self.shared, p.GROUP, self.id, min_idle_time=int(self.cfg.reclaim_idle_s * 1000), start_id="0-0", count=free
            )
            for msg_id, fields in claimed:
                if fields:
                    RECLAIMED.labels(self.cfg.model, "stalled").inc()
                    fields["attempt"] = int(fields.get("attempt") or 1) + 1
                    self._spawn(self.shared, msg_id, fields)
                    taken += 1
        cutoff = int((time.time() - self.cfg.dead_after_s) * 1000)
        dead_ids = set(await self.r.zrangebyscore(p.live_set(self.cfg.model), 0, cutoff))
        dead_ids |= set(await self.r.zrangebyscore(p.draining_set(self.cfg.model), 0, cutoff))
        for dead in sorted(dead_ids):
            if dead == self.id or not await self.r.set(f"ig:rescue:{dead}", self.id, nx=True, ex=30):
                continue
            taken += await self._rescue(dead)
        return taken

    async def _rescue(self, dead: str) -> int:
        """Move a dead worker's queued and in-flight requests to the shared queue."""
        stream = p.worker_stream(dead)
        moved = 0
        try:
            # pending: read by the dead worker, maybe half answered; run again as a new attempt
            _, claimed, _ = await self.r.xautoclaim(stream, p.GROUP, self.id, min_idle_time=0, start_id="0-0", count=1000)
            for _, fields in claimed:
                if fields:
                    fields["attempt"] = int(fields.get("attempt") or 1) + 1
            # unread: never delivered, nothing was sent for them
            unread = await self.r.xreadgroup(p.GROUP, self.id, {stream: ">"}, count=1000)
            entries = list(claimed) + [e for _, es in (unread or []) for e in es]
            for msg_id, fields in entries:
                if fields:
                    await self.r.xadd(self.shared, fields)
                    moved += 1
                await self.r.xack(stream, p.GROUP, msg_id)
            await self.r.delete(stream, p.worker_hash(dead))
            await self._forget(dead)
        except ResponseError as e:
            if "NOGROUP" not in str(e) and "no such key" not in str(e):
                raise
            await self._forget(dead)
        if moved:
            RECLAIMED.labels(self.cfg.model, "dead_worker").inc(moved)
            log.info("rescued %d requests from dead worker %s", moved, dead)
        return moved

    async def _forget(self, worker: str) -> None:
        await self.r.zrem(p.live_set(self.cfg.model), worker)
        await self.r.zrem(p.draining_set(self.cfg.model), worker)

    async def _reclaim_loop(self) -> None:
        while not self.stopping.is_set():
            try:
                await self.reclaim()
            except Exception as e:
                log.warning("reclaim failed: %s", e)
            try:
                await asyncio.wait_for(self.stopping.wait(), timeout=max(1.0, self.cfg.dead_after_s / 3))
            except asyncio.TimeoutError:
                pass

    async def run(self) -> None:
        await self._ensure_group(self.own)
        await self._ensure_group(self.shared)
        await self.backend.start()
        await self.heartbeat()
        log.info("worker %s serving %s (backend %s, capacity %d)", self.id, self.cfg.model, self.cfg.backend, self.cfg.capacity)
        bg = [asyncio.create_task(self._heartbeat_loop()), asyncio.create_task(self._reclaim_loop())]
        try:
            await self._consume()
        finally:
            await self.drain()
            for t in bg:
                t.cancel()

    async def stop(self) -> None:
        self.stopping.set()

    async def drain(self) -> None:
        """Leave the live set, finish in-flight work, hand back what's still queued here."""
        await self.r.zadd(p.draining_set(self.cfg.model), {self.id: int(time.time() * 1000)})
        await self.r.zrem(p.live_set(self.cfg.model), self.id)
        if self.tasks:
            done, pending = await asyncio.wait(set(self.tasks), timeout=self.cfg.shutdown_grace_s)
            for t in pending:
                t.cancel()
        moved = 0
        while True:
            res = await self.r.xreadgroup(p.GROUP, self.id, {self.own: ">"}, count=500)
            entries = [e for _, es in (res or []) for e in es]
            if not entries:
                break
            for msg_id, fields in entries:
                await self.r.xadd(self.shared, fields)
                await self.r.xack(self.own, p.GROUP, msg_id)
                moved += 1
        self.drained = True
        await self.r.delete(self.own, p.worker_hash(self.id))
        await self.r.zrem(p.draining_set(self.cfg.model), self.id)
        if moved:
            log.info("handed %d queued requests back to the shared queue", moved)
