"""Entry point: python -m infergate_worker.main (or the infergate-worker script)."""

from __future__ import annotations

import asyncio
import logging
import signal

from prometheus_client import start_http_server

from .backends import make
from .config import Config
from .worker import Worker


async def amain() -> None:
    cfg = Config()
    start_http_server(cfg.metrics_port)
    worker = Worker(cfg, make(cfg))
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(worker.stop()))
    await worker.run()


def run() -> None:
    logging.basicConfig(level=logging.INFO, format='{"level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}')
    asyncio.run(amain())


if __name__ == "__main__":
    run()
