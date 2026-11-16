import os
import shutil
import socket
import subprocess
import tempfile
import time

import psycopg
import pytest

os.environ["IG_REGISTRY_AUTOAPP"] = "0"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def pg_dsn():
    """$DATABASE_URL (CI service), or a throwaway PostgreSQL cluster."""
    if os.environ.get("DATABASE_URL"):
        yield os.environ["DATABASE_URL"]
        return
    bindir = None
    for d in sorted(os.listdir("/usr/lib/postgresql"), reverse=True) if os.path.isdir("/usr/lib/postgresql") else []:
        bindir = f"/usr/lib/postgresql/{d}/bin"
        break
    if not bindir or not os.path.exists(f"{bindir}/initdb"):
        pytest.skip("no DATABASE_URL and no local PostgreSQL")
    data = tempfile.mkdtemp(prefix="igpg-")
    port = _free_port()
    as_pg = ["runuser", "-u", "postgres", "--"] if os.geteuid() == 0 else []
    if as_pg:
        shutil.chown(data, "postgres", "postgres")
    subprocess.run(as_pg + [f"{bindir}/initdb", "-D", data, "-U", "postgres", "--auth=trust"], check=True, stdout=subprocess.DEVNULL)
    proc = subprocess.Popen(
        as_pg + [f"{bindir}/postgres", "-D", data, "-p", str(port), "-k", data, "-c", "listen_addresses=127.0.0.1", "-c", "fsync=off"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    dsn = f"postgresql://postgres@127.0.0.1:{port}/postgres"
    for _ in range(100):
        try:
            psycopg.connect(dsn).close()
            break
        except psycopg.OperationalError:
            time.sleep(0.1)
    yield dsn
    proc.terminate()
    proc.wait()
    shutil.rmtree(data, ignore_errors=True)


@pytest.fixture
def db(pg_dsn):
    """A fresh database per test."""
    name = f"t{int(time.time() * 1e6)}"
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        c.execute(f"create database {name}")
    yield pg_dsn.rsplit("/", 1)[0] + "/" + name
    with psycopg.connect(pg_dsn, autocommit=True) as c:
        c.execute(f"drop database {name} with (force)")
