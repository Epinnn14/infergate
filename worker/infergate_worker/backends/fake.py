"""A simulated accelerator, so the platform can be load-tested without a GPU.

It behaves like an LLM server doing continuous batching:

* every active sequence gets one token per decode step;
* a step takes longer the more sequences share it (``batch_penalty``), so throughput
  rises with batch size while per-request speed drops, as on a real GPU;
* a new request first pays a prefill cost proportional to its prompt, except the part
  already in the prefix cache: a request whose conversation prefix this worker has seen
  recently prefills ~85% faster. This is what affinity routing in the gateway exploits.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections import OrderedDict

from . import Cancelled, Emit, Job, Result

WORDS = (
    "the platform routes each request to a worker that already holds its prompt prefix in cache "
    "so prefill is short and the first token arrives sooner while the queue absorbs bursts and "
    "autoscaling adds replicas when waiting requests pile up"
).split()


def prompt_tokens(messages: list[dict]) -> int:
    return sum(len(m.get("content", "")) // 4 + 4 for m in messages)


class _Seq:
    __slots__ = ("job", "left", "out", "n", "seed")

    def __init__(self, job: Job, length: int):
        self.job = job
        self.left = length
        self.out: asyncio.Queue[str | None] = asyncio.Queue()
        self.n = 0
        self.seed = int(hashlib.sha1(job.id.encode()).hexdigest()[:8], 16)


class FakeBackend:
    def __init__(self, cfg):
        self.cfg = cfg
        self.active: list[_Seq] = []
        self.cache: OrderedDict[str, None] = OrderedDict()
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None
        self.steps = 0
        self.batch_sum = 0

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    def stats(self) -> dict:
        return {"active": len(self.active), "cached_prefixes": len(self.cache)}

    def _length(self, job: Job) -> int:
        # replies of 48-128 tokens, fixed per request id, never above max_tokens
        h = int(hashlib.sha1(job.id.encode()).hexdigest()[:6], 16)
        return max(1, min(job.max_tokens, 48 + h % 81))

    async def _loop(self) -> None:
        base = 1.0 / max(self.cfg.fake_tokens_per_s, 1e-3)
        while True:
            if not self.active:
                self._wake.clear()
                await self._wake.wait()
            n = len(self.active)
            await asyncio.sleep(base * (1 + self.cfg.fake_batch_penalty * (n - 1)))
            self.steps += 1
            self.batch_sum += n
            for s in list(self.active):
                w = WORDS[(s.seed + s.n) % len(WORDS)]
                s.out.put_nowait(w if s.n == 0 else " " + w)
                s.n += 1
                s.left -= 1
                if s.left <= 0:
                    s.out.put_nowait(None)
                    self.active.remove(s)

    async def generate(self, job: Job, emit: Emit, cancelled: Cancelled) -> Result:
        pt = prompt_tokens(job.messages)
        hit = bool(job.prefix) and job.prefix in self.cache
        if hit:
            self.cache.move_to_end(job.prefix)
        # prefill: the cached prefix is skipped, only the new turn is processed
        await asyncio.sleep(pt * (0.15 if hit else 1.0) * self.cfg.fake_prefill_ms_per_token / 1000)
        if job.prefix:
            self.cache[job.prefix] = None
            while len(self.cache) > self.cfg.fake_prefix_cache:
                self.cache.popitem(last=False)

        seq = _Seq(job, self._length(job))
        self.active.append(seq)
        self._wake.set()
        out = 0
        reason = "length" if seq.left >= job.max_tokens else "stop"
        try:
            while True:
                tok = await seq.out.get()
                if tok is None:
                    break
                await emit(tok)
                out += 1
                if out % 8 == 0 and await cancelled():
                    reason = "cancelled"
                    break
        finally:
            if seq in self.active:
                self.active.remove(seq)
        return Result(prompt_tokens=pt, output_tokens=out, finish_reason=reason, cache_hit=hit)
