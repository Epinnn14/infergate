import json

import httpx
import pytest

from infergate_worker.backends import Job
from infergate_worker.backends.openai import OpenAIBackend
from infergate_worker.config import Config


def sse(chunks):
    body = "".join(f"data: {json.dumps(c)}\n\n" for c in chunks) + "data: [DONE]\n\n"
    return body.encode()


def engine(seen, words=("Halo", " dari", " Ollama", "!"), usage=True, status=200):
    """A stand-in OpenAI-compatible engine."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json={"data": [{"id": "llama3.2:1b"}]})
        seen.append(json.loads(request.content))
        if status != 200:
            return httpx.Response(status, json={"error": {"message": "model not found"}})
        chunks = [{"choices": [{"delta": {"content": w}, "finish_reason": None}]} for w in words]
        chunks.append({"choices": [{"delta": {}, "finish_reason": "stop"}]})
        if usage:
            chunks.append({"choices": [], "usage": {"prompt_tokens": 11, "completion_tokens": len(words)}})
        return httpx.Response(200, content=sse(chunks), headers={"content-type": "text/event-stream"})

    return httpx.MockTransport(handler)


def cfg(**kw):
    c = Config()
    c.upstream_url = "http://engine/v1"
    c.upstream_model = "llama3.2:1b"
    c.capacity = 2
    for k, v in kw.items():
        setattr(c, k, v)
    return c


async def run(backend, cancel_after=None):
    got = []

    async def emit(piece):
        got.append(piece)

    async def cancelled():
        return cancel_after is not None and len(got) >= cancel_after

    job = Job(id="r1", model="v1", messages=[{"role": "user", "content": "hai"}], max_tokens=32, temperature=0.2)
    res = await backend.generate(job, emit, cancelled)
    return got, res


async def test_streams_tokens_and_usage():
    seen = []
    b = OpenAIBackend(cfg(), transport=engine(seen))
    await b.start()
    got, res = await run(b)
    assert "".join(got) == "Halo dari Ollama!"
    assert (res.prompt_tokens, res.output_tokens, res.finish_reason) == (11, 4, "stop")
    req = seen[0]
    assert req["model"] == "llama3.2:1b" and req["stream"] is True and req["max_tokens"] == 32
    assert req["messages"] == [{"role": "user", "content": "hai"}]


async def test_counts_chunks_without_usage():
    b = OpenAIBackend(cfg(), transport=engine([], usage=False))
    await b.start()
    _, res = await run(b)
    assert res.output_tokens == 4 and res.prompt_tokens > 0


async def test_upstream_error_is_raised():
    b = OpenAIBackend(cfg(), transport=engine([], status=404))
    await b.start()
    with pytest.raises(RuntimeError, match="404"):
        await run(b)
    assert b.stats()["upstream_errors"] == 1


async def test_cancel_stops_reading():
    words = [f" w{i}" for i in range(40)]
    b = OpenAIBackend(cfg(), transport=engine([], words=words))
    await b.start()
    got, res = await run(b, cancel_after=8)
    assert res.finish_reason == "cancelled" and len(got) == 8


async def test_waits_for_the_engine_to_come_up():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] < 3:
            raise httpx.ConnectError("engine still loading")
        return httpx.Response(200, json={"data": []})

    b = OpenAIBackend(cfg(upstream_wait_s=10), transport=httpx.MockTransport(handler))
    await b.start()
    assert calls["n"] == 3


async def test_gives_up_on_an_engine_that_never_starts():
    def handler(request):
        raise httpx.ConnectError("nothing listening")

    b = OpenAIBackend(cfg(upstream_wait_s=0.5), transport=httpx.MockTransport(handler))
    with pytest.raises(RuntimeError, match="not ready"):
        await b.start()
