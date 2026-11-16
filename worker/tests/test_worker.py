import asyncio
import json
import time

import pytest
import redis.asyncio as aioredis

from infergate_worker import protocol as p
from infergate_worker.backends.fake import FakeBackend
from infergate_worker.config import Config
from infergate_worker.worker import Worker


def cfg(url, wid="w1", **kw):
    c = Config()
    c.redis_url = url
    c.model = "tiny-v1"
    c.worker_id = wid
    c.capacity = kw.pop("capacity", 4)
    c.fake_tokens_per_s = kw.pop("tps", 2000)
    c.fake_prefill_ms_per_token = kw.pop("prefill", 0.5)
    c.dead_after_s = kw.pop("dead_after", 2)
    c.reclaim_idle_s = kw.pop("reclaim_idle", 1)
    c.heartbeat_s = 0.2
    c.shutdown_grace_s = 5
    for k, v in kw.items():
        setattr(c, k, v)
    return c


async def enqueue(r, stream, rid, prefix="", max_tokens=16, deadline_s=30, messages=None):
    now = time.time()
    await r.xadd(stream, {
        "id": rid, "model": "tiny-v1", "reply": f"ig:r:{rid}",
        "messages": json.dumps(messages or [{"role": "system", "content": "x" * 400}, {"role": "user", "content": "hi"}]),
        "max_tokens": max_tokens, "temperature": 1, "prefix": prefix,
        "deadline_ms": int((now + deadline_s) * 1000), "enqueued_ms": int(now * 1000),
    })


async def collect(r, rid, timeout=5.0):
    """Read the reply stream like the gateway does, until done or error."""
    last, text, end = "0", [], None
    t0 = time.time()
    while time.time() - t0 < timeout:
        res = await r.xread({f"ig:r:{rid}": last}, block=200)
        for _, entries in res or []:
            for mid, f in entries:
                last = mid
                if f["type"] == "token":
                    text.append(f["text"])
                elif f["type"] == "restart":
                    text = []  # another worker runs it again from the start
                else:
                    return "".join(text), f
    raise AssertionError(f"no reply for {rid}")


@pytest.fixture
async def rds(clean):
    r = aioredis.from_url(clean, decode_responses=True)
    yield r
    await r.aclose()


async def start(c, rds):
    w = Worker(c, FakeBackend(c), rds)
    task = asyncio.create_task(w.run())
    for _ in range(50):
        if await rds.zscore(p.live_set(c.model), c.worker_id):
            break
        await asyncio.sleep(0.02)
    return w, task


async def test_serves_from_shared_and_own_streams(rds, clean):
    w, task = await start(cfg(clean), rds)
    await enqueue(rds, p.shared_stream("tiny-v1"), "a")
    await enqueue(rds, p.worker_stream("w1"), "b", prefix="pfx")
    text_a, done_a = await collect(rds, "a")
    text_b, done_b = await collect(rds, "b")
    assert text_a and done_a["type"] == "done" and int(done_a["output_tokens"]) == 16
    assert done_a["finish_reason"] == "length" and done_a["worker"] == "w1"
    assert int(done_b["prompt_tokens"]) > 100
    # own stream is trimmed right after the ack, so XLEN means "waiting"
    for _ in range(50):
        if await rds.xlen(p.worker_stream("w1")) == 0:
            break
        await asyncio.sleep(0.01)
    assert await rds.xlen(p.worker_stream("w1")) == 0
    h = await rds.hgetall(p.worker_hash("w1"))
    assert h["capacity"] == "4" and h["model"] == "tiny-v1"
    await w.stop()
    await task


async def test_prefix_cache_makes_prefill_faster(rds, clean):
    # slow prefill so the difference is easy to see
    w, task = await start(cfg(clean, prefill=5, tps=4000), rds)
    msgs = [{"role": "system", "content": "s" * 2000}, {"role": "user", "content": "hi"}]
    t0 = time.time()
    await enqueue(rds, p.worker_stream("w1"), "cold", prefix="conv", max_tokens=1, messages=msgs)
    _, cold = await collect(rds, "cold")
    cold_s = time.time() - t0
    t0 = time.time()
    await enqueue(rds, p.worker_stream("w1"), "warm", prefix="conv", max_tokens=1, messages=msgs)
    _, warm = await collect(rds, "warm")
    warm_s = time.time() - t0
    assert cold["cache_hit"] == "0" and warm["cache_hit"] == "1"
    assert warm_s < cold_s * 0.5, (cold_s, warm_s)
    await w.stop()
    await task


async def test_cancel_and_deadline(rds, clean):
    w, task = await start(cfg(clean, tps=50), rds)
    await enqueue(rds, p.shared_stream("tiny-v1"), "long", max_tokens=500)
    await asyncio.sleep(0.3)
    await rds.set(p.cancel_key("long"), "1")
    _, done = await collect(rds, "long")
    assert done["finish_reason"] == "cancelled" and int(done["output_tokens"]) < 100
    await enqueue(rds, p.shared_stream("tiny-v1"), "late", deadline_s=-1)
    _, err = await collect(rds, "late")
    assert err["type"] == "error" and "deadline" in err["text"]
    await w.stop()
    await task


async def test_capacity_is_respected(rds, clean):
    w, task = await start(cfg(clean, capacity=3, tps=100), rds)
    for i in range(9):
        await enqueue(rds, p.shared_stream("tiny-v1"), f"r{i}", max_tokens=30)
    peak = 0
    for _ in range(40):
        peak = max(peak, len(w.tasks))
        await asyncio.sleep(0.02)
    assert peak == 3
    for i in range(9):
        await collect(rds, f"r{i}", timeout=10)
    await w.stop()
    await task


async def test_dead_worker_requests_are_rescued(rds, clean):
    # a worker that read two requests from its own stream and then died
    await rds.xgroup_create(p.worker_stream("ghost"), p.GROUP, id="0", mkstream=True)
    await enqueue(rds, p.worker_stream("ghost"), "x1", prefix="a")
    await enqueue(rds, p.worker_stream("ghost"), "x2", prefix="b")
    await rds.xreadgroup(p.GROUP, "ghost", {p.worker_stream("ghost"): ">"}, count=1)  # x1 now pending at ghost
    await rds.zadd(p.live_set("tiny-v1"), {"ghost": int((time.time() - 60) * 1000)})
    w, task = await start(cfg(clean, dead_after=1), rds)
    _, d1 = await collect(rds, "x1", timeout=8)
    _, d2 = await collect(rds, "x2", timeout=8)
    assert d1["worker"] == "w1" and d2["worker"] == "w1"
    assert await rds.zscore(p.live_set("tiny-v1"), "ghost") is None
    assert not await rds.exists(p.worker_stream("ghost"))
    await w.stop()
    await task


async def test_worker_killed_while_draining_is_rescued(rds, clean):
    # it had left the live set to drain, then was SIGKILLed (grace period over) mid-drain
    await rds.xgroup_create(p.worker_stream("halfway"), p.GROUP, id="0", mkstream=True)
    await enqueue(rds, p.worker_stream("halfway"), "y1", prefix="a")
    await rds.xreadgroup(p.GROUP, "halfway", {p.worker_stream("halfway"): ">"}, count=1)
    await rds.zadd(p.draining_set("tiny-v1"), {"halfway": int((time.time() - 60) * 1000)})
    w, task = await start(cfg(clean, dead_after=1), rds)
    _, d1 = await collect(rds, "y1", timeout=8)
    assert d1["worker"] == "w1"
    assert await rds.zscore(p.draining_set("tiny-v1"), "halfway") is None
    await w.stop()
    await task


async def test_stalled_shared_messages_are_reclaimed(rds, clean):
    await rds.xgroup_create(p.shared_stream("tiny-v1"), p.GROUP, id="0", mkstream=True)
    await enqueue(rds, p.shared_stream("tiny-v1"), "stuck")
    await rds.xreadgroup(p.GROUP, "crashed", {p.shared_stream("tiny-v1"): ">"}, count=1)
    await asyncio.sleep(1.1)  # idle longer than reclaim_idle
    w, task = await start(cfg(clean, reclaim_idle=1, dead_after=3), rds)
    _, done = await collect(rds, "stuck", timeout=8)
    assert done["type"] == "done"
    # it was a second attempt, so the gateway was told to drop the first one's output
    events = await rds.xrange("ig:r:stuck")
    assert events[0][1]["type"] == "restart"
    await w.stop()
    await task


async def test_graceful_stop_hands_back_queued_work(rds, clean):
    w, task = await start(cfg(clean, capacity=1, tps=40), rds)
    for i in range(4):
        await enqueue(rds, p.worker_stream("w1"), f"q{i}", prefix="p", max_tokens=10)
    await asyncio.sleep(0.15)
    await w.stop()
    await task
    assert await rds.zscore(p.live_set("tiny-v1"), "w1") is None
    assert await rds.zscore(p.draining_set("tiny-v1"), "w1") is None
    # the one in flight finished; the rest wait in the shared queue for another worker
    _, d0 = await collect(rds, "q0")
    assert d0["type"] == "done"
    assert await rds.xlen(p.shared_stream("tiny-v1")) == 3
    w2, task2 = await start(cfg(clean, wid="w2"), rds)
    for i in range(1, 4):
        _, d = await collect(rds, f"q{i}")
        assert d["worker"] == "w2"
    await w2.stop()
    await task2
