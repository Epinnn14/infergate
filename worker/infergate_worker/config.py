"""Worker settings from the environment (set by the Helm chart)."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field


def _f(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def _default_id() -> str:
    host = socket.gethostname()
    prefix = os.environ.get("IG_WORKER_PREFIX", "").strip()
    return f"{prefix}-{host[:6]}" if prefix else host


@dataclass
class Config:
    redis_url: str = field(default_factory=lambda: os.environ.get("IG_REDIS_URL", "redis://127.0.0.1:6379/0"))
    model: str = field(default_factory=lambda: os.environ.get("IG_MODEL", "tiny-v1"))
    # IG_WORKER_ID, or IG_WORKER_PREFIX plus a short host id (replicas of one compose service), or the hostname
    worker_id: str = field(default_factory=lambda: os.environ.get("IG_WORKER_ID") or _default_id())
    backend: str = field(default_factory=lambda: os.environ.get("IG_BACKEND", "fake"))
    # sequences generated at the same time (continuous batching slots)
    capacity: int = field(default_factory=lambda: int(_f("IG_CAPACITY", 8)))
    metrics_port: int = field(default_factory=lambda: int(_f("IG_METRICS_PORT", 9100)))
    heartbeat_s: float = field(default_factory=lambda: _f("IG_HEARTBEAT_S", 1.0))
    # a worker silent this long is dead: its queued requests are taken over
    dead_after_s: float = field(default_factory=lambda: _f("IG_DEAD_AFTER_S", 15.0))
    # unacknowledged shared-queue messages idle this long are reclaimed
    reclaim_idle_s: float = field(default_factory=lambda: _f("IG_RECLAIM_IDLE_S", 60.0))
    shutdown_grace_s: float = field(default_factory=lambda: _f("IG_SHUTDOWN_GRACE_S", 25.0))

    # fake backend: a simulated accelerator, for load tests without a GPU
    fake_tokens_per_s: float = field(default_factory=lambda: _f("IG_FAKE_TOKENS_PER_S", 40.0))
    fake_batch_penalty: float = field(default_factory=lambda: _f("IG_FAKE_BATCH_PENALTY", 0.08))
    fake_prefill_ms_per_token: float = field(default_factory=lambda: _f("IG_FAKE_PREFILL_MS_PER_TOKEN", 0.6))
    fake_prefix_cache: int = field(default_factory=lambda: int(_f("IG_FAKE_PREFIX_CACHE", 256)))

    # hf backend
    hf_model: str = field(default_factory=lambda: os.environ.get("IG_HF_MODEL", "Qwen/Qwen2.5-0.5B-Instruct"))
    hf_device: str = field(default_factory=lambda: os.environ.get("IG_HF_DEVICE", "cpu"))
