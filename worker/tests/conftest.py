import os
import shutil
import socket
import subprocess
import time

import pytest
import redis


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def redis_url():
    """$REDIS_URL (CI service), or a throwaway redis-server."""
    url = os.environ.get("REDIS_URL")
    if url:
        yield url
        return
    binary = shutil.which("redis-server")
    if not binary:
        pytest.skip("no REDIS_URL and no redis-server")
    port = _free_port()
    proc = subprocess.Popen([binary, "--port", str(port), "--save", "", "--appendonly", "no"], stdout=subprocess.DEVNULL)
    url = f"redis://127.0.0.1:{port}/0"
    for _ in range(100):
        try:
            redis.Redis.from_url(url).ping()
            break
        except redis.ConnectionError:
            time.sleep(0.05)
    yield url
    proc.kill()
    proc.wait()


@pytest.fixture
def clean(redis_url):
    redis.Redis.from_url(redis_url).flushdb()
    return redis_url
