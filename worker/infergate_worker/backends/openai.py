"""Any OpenAI-compatible server as the model engine: Ollama, vLLM, SGLang, LM Studio,
llama.cpp server, TGI, or a hosted API.

The worker keeps doing what infergate needs (queues, affinity, rescue, cancellation, metrics)
and the engine does the generation. Point IG_UPSTREAM_URL at the engine's /v1 base, e.g.
http://127.0.0.1:11434/v1 for Ollama, or http://localhost:8000/v1 for a vLLM sidecar in the
same pod. IG_CAPACITY should match how many requests the engine runs at once.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

import httpx

from . import Cancelled, Emit, Job, Result
from .fake import prompt_tokens

log = logging.getLogger("infergate.openai")


class OpenAIBackend:
    def __init__(self, cfg, transport: httpx.AsyncBaseTransport | None = None):
        self.cfg = cfg
        self.sem = asyncio.Semaphore(max(1, cfg.capacity))
        self.transport = transport
        self.client: httpx.AsyncClient | None = None
        self.errors = 0

    async def start(self) -> None:
        headers = {"Authorization": f"Bearer {self.cfg.upstream_key}"} if self.cfg.upstream_key else {}
        self.client = httpx.AsyncClient(
            base_url=self.cfg.upstream_url.rstrip("/"),
            headers=headers,
            timeout=httpx.Timeout(10.0, read=self.cfg.upstream_read_timeout_s),
            transport=self.transport,
        )
        await self._wait_ready()

    async def _wait_ready(self) -> None:
        """Don't join the cluster (heartbeat) before the engine answers: a vLLM sidecar can take
        minutes to load its model, and requests routed here meanwhile would only fail."""
        deadline = time.monotonic() + self.cfg.upstream_wait_s
        last_log = 0.0
        while True:
            try:
                res = await self.client.get("/models", timeout=5.0)
                if res.status_code < 500:
                    return
                why = f"HTTP {res.status_code}"
            except httpx.HTTPError as e:
                why = type(e).__name__
            if time.monotonic() > deadline:
                raise RuntimeError(f"engine at {self.cfg.upstream_url} not ready after {self.cfg.upstream_wait_s:.0f}s ({why})")
            if time.monotonic() - last_log > 10:
                log.info("waiting for engine at %s (%s)", self.cfg.upstream_url, why)
                last_log = time.monotonic()
            await asyncio.sleep(1)

    def stats(self) -> dict:
        return {"upstream_model": self.cfg.upstream_model, "upstream_errors": self.errors}

    async def generate(self, job: Job, emit: Emit, cancelled: Cancelled) -> Result:
        body = {
            "model": self.cfg.upstream_model,
            "messages": job.messages,
            "max_tokens": job.max_tokens,
            "temperature": job.temperature,
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        out, reason, usage = 0, "stop", None
        async with self.sem:
            try:
                async with self.client.stream("POST", "/chat/completions", json=body) as res:
                    if res.status_code != 200:
                        detail = (await res.aread()).decode(errors="replace")[:300]
                        raise RuntimeError(f"upstream returned {res.status_code}: {detail}")
                    async for line in res.aiter_lines():
                        if not line.startswith("data:"):
                            continue
                        data = line[5:].strip()
                        if data == "[DONE]":
                            break
                        chunk = json.loads(data)
                        if chunk.get("usage"):
                            usage = chunk["usage"]
                        for choice in chunk.get("choices") or []:
                            piece = (choice.get("delta") or {}).get("content")
                            if piece:
                                await emit(piece)
                                out += 1
                            if choice.get("finish_reason"):
                                reason = choice["finish_reason"]
                        if out and out % 8 == 0 and await cancelled():
                            reason = "cancelled"
                            break  # leaving the block closes the connection; the engine stops
            except Exception:
                self.errors += 1
                raise
        return Result(
            prompt_tokens=(usage or {}).get("prompt_tokens") or prompt_tokens(job.messages),
            # without usage, count stream chunks (engines send about one token per chunk)
            output_tokens=(usage or {}).get("completion_tokens") or out,
            finish_reason=reason,
        )
