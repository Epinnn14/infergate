"""Model backends: something that turns a chat into tokens."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Awaitable, Callable, Protocol


@dataclass
class Job:
    id: str
    model: str
    messages: list[dict]
    max_tokens: int = 256
    temperature: float = 1.0
    prefix: str = ""


@dataclass
class Result:
    prompt_tokens: int = 0
    output_tokens: int = 0
    finish_reason: str = "stop"
    cache_hit: bool = False
    extra: dict = field(default_factory=dict)


Emit = Callable[[str], Awaitable[None]]
Cancelled = Callable[[], Awaitable[bool]]


class Backend(Protocol):
    async def start(self) -> None: ...

    async def generate(self, job: Job, emit: Emit, cancelled: Cancelled) -> Result: ...

    def stats(self) -> dict: ...


def make(cfg) -> Backend:
    if cfg.backend == "fake":
        from .fake import FakeBackend

        return FakeBackend(cfg)
    if cfg.backend == "hf":
        from .hf import HFBackend

        return HFBackend(cfg)
    if cfg.backend == "openai":
        from .openai import OpenAIBackend

        return OpenAIBackend(cfg)
    raise ValueError(f"unknown backend {cfg.backend!r} (fake, hf, openai)")
