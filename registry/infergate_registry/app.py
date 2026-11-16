"""The registry API.

Admin endpoints (bearer IG_ADMIN_TOKEN) change models, versions, traffic weights and API
keys. The gateway reads everything it needs from GET /v1/routes (bearer IG_ROUTES_TOKEN),
which carries a revision number that rises with every change.

Rolling out a new model version is: add the version with weight 0, deploy its workers,
then shift weight (90/10 → 50/50 → 0/100). Gateways pick up the change within one poll.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
from contextlib import asynccontextmanager
from importlib import resources

from fastapi import Depends, FastAPI, Header, HTTPException
from psycopg import errors
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

NAME = r"^[a-z0-9][a-z0-9._-]{0,62}$"


# ------------------------------------------------------------------ database


def migrate(pool: ConnectionPool) -> None:
    """Apply migrations/*.sql in order, once, under an advisory lock (several replicas may start together)."""
    files = sorted(f for f in resources.files("infergate_registry").joinpath("migrations").iterdir() if f.name.endswith(".sql"))
    with pool.connection() as conn:
        conn.execute("select pg_advisory_lock(7171)")
        try:
            conn.execute("create table if not exists schema_migrations (version text primary key, applied_at timestamptz not null default now())")
            done = {r["version"] for r in conn.execute("select version from schema_migrations")}
            for f in files:
                if f.name in done:
                    continue
                with conn.transaction():
                    conn.execute(f.read_text())
                    conn.execute("insert into schema_migrations (version) values (%s)", (f.name,))
            conn.commit()
        finally:
            conn.execute("select pg_advisory_unlock(7171)")
            conn.commit()


def bootstrap(pool: ConnectionPool, spec: dict) -> None:
    """Create models, versions and keys from IG_BOOTSTRAP on an empty registry (Helm installs)."""
    with pool.connection() as conn, conn.transaction():
        if conn.execute("select count(*) as n from models").fetchone()["n"]:
            return
        for m in spec.get("models", []):
            conn.execute("insert into models (name, description) values (%s, %s)", (m["name"], m.get("description", "")))
            for v in m.get("versions", []):
                conn.execute(
                    "insert into versions (name, model, backend, weight) values (%s, %s, %s, %s)",
                    (v["name"], m["name"], v.get("backend", "fake"), v.get("weight", 0)),
                )
        for k in spec.get("keys", []):
            conn.execute(
                "insert into api_keys (id, name, key_hash, rpm, tpm, models) values (%s, %s, %s, %s, %s, %s)",
                (_key_id(), k["name"], hash_key(k["key"]), k.get("rpm", 0), k.get("tpm", 0), k.get("models", [])),
            )
        conn.execute("insert into revisions (what) values ('bootstrap')")


def hash_key(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def _key_id() -> str:
    return "key_" + secrets.token_hex(6)


def bump(conn, what: str) -> int:
    return conn.execute("insert into revisions (what) values (%s) returning id", (what,)).fetchone()["id"]


# ------------------------------------------------------------------ request bodies


class ModelIn(BaseModel):
    name: str = Field(pattern=NAME)
    description: str = ""


class VersionIn(BaseModel):
    name: str = Field(pattern=NAME)
    backend: str = "fake"
    config: dict = {}
    weight: int = Field(default=0, ge=0, le=100)


class KeyIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    rpm: int = Field(default=60, ge=0)
    tpm: int = Field(default=100_000, ge=0)
    models: list[str] = []


# ------------------------------------------------------------------ app


def create_app(dsn: str | None = None, admin_token: str | None = None, routes_token: str | None = None) -> FastAPI:
    dsn = dsn or os.environ.get("IG_DATABASE_URL", "postgresql://infergate:infergate@127.0.0.1:5432/infergate")
    admin_token = admin_token if admin_token is not None else os.environ.get("IG_ADMIN_TOKEN", "")
    routes_token = routes_token if routes_token is not None else os.environ.get("IG_ROUTES_TOKEN", "")
    pool = ConnectionPool(dsn, min_size=1, max_size=int(os.environ.get("IG_DB_POOL", "10")), open=False, kwargs={"row_factory": dict_row})

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool.open(wait=True, timeout=30)
        migrate(pool)
        if os.environ.get("IG_BOOTSTRAP"):
            bootstrap(pool, json.loads(os.environ["IG_BOOTSTRAP"]))
        yield
        pool.close()

    app = FastAPI(title="infergate registry", version="0.1.0", lifespan=lifespan)
    app.state.pool = pool

    def need(token: str):
        def check(authorization: str = Header(default="")):
            if not token:
                return  # no token configured: open (local development)
            if not secrets.compare_digest(authorization, f"Bearer {token}"):
                raise HTTPException(401, "bad token")

        return check

    admin = Depends(need(admin_token))
    reader = Depends(need(routes_token))

    @app.get("/healthz")
    def healthz():
        with pool.connection() as conn:
            conn.execute("select 1")
        return {"ok": True}

    @app.get("/v1/routes", dependencies=[reader])
    def routes():
        with pool.connection() as conn:
            rev = conn.execute("select max(id) as r from revisions").fetchone()["r"]
            models = conn.execute("select name from models order by name").fetchall()
            versions = conn.execute("select name, model, weight from versions order by created_at, name").fetchall()
            keys = conn.execute(
                "select id, name, key_hash as hash, rpm, tpm, models from api_keys where revoked_at is null order by created_at"
            ).fetchall()
        by_model: dict[str, list] = {m["name"]: [] for m in models}
        for v in versions:
            by_model[v["model"]].append({"name": v["name"], "weight": v["weight"]})
        return {
            "revision": rev,
            "models": [{"name": n, "versions": vs} for n, vs in by_model.items()],
            "keys": [dict(k) for k in keys],
        }

    # ---- models


    @app.get("/v1/models", dependencies=[admin])
    def list_models():
        with pool.connection() as conn:
            rows = conn.execute(
                """select m.name, m.description, coalesce(json_agg(json_build_object('name', v.name, 'backend', v.backend,
                   'weight', v.weight) order by v.created_at) filter (where v.name is not null), '[]') as versions
                   from models m left join versions v on v.model = m.name group by m.name order by m.name"""
            ).fetchall()
        return rows

    @app.post("/v1/models", status_code=201, dependencies=[admin])
    def create_model(m: ModelIn):
        try:
            with pool.connection() as conn, conn.transaction():
                conn.execute("insert into models (name, description) values (%s, %s)", (m.name, m.description))
                rev = bump(conn, f"model {m.name} created")
        except errors.UniqueViolation:
            raise HTTPException(409, f"model {m.name} exists")
        return {"name": m.name, "revision": rev}

    @app.delete("/v1/models/{name}", dependencies=[admin])
    def delete_model(name: str):
        with pool.connection() as conn, conn.transaction():
            if not conn.execute("delete from models where name = %s", (name,)).rowcount:
                raise HTTPException(404, "no such model")
            rev = bump(conn, f"model {name} deleted")
        return {"revision": rev}

    # ---- versions and weights


    @app.post("/v1/models/{model}/versions", status_code=201, dependencies=[admin])
    def add_version(model: str, v: VersionIn):
        try:
            with pool.connection() as conn, conn.transaction():
                conn.execute(
                    "insert into versions (name, model, backend, config, weight) values (%s, %s, %s, %s, %s)",
                    (v.name, model, v.backend, json.dumps(v.config), v.weight),
                )
                rev = bump(conn, f"version {v.name} of {model} added with weight {v.weight}")
        except errors.ForeignKeyViolation:
            raise HTTPException(404, "no such model")
        except errors.UniqueViolation:
            raise HTTPException(409, f"version {v.name} exists")
        return {"name": v.name, "revision": rev}

    @app.put("/v1/models/{model}/weights", dependencies=[admin])
    def set_weights(model: str, weights: dict[str, int]):
        """{"qwen-v1": 90, "qwen-v2": 10}: versions not listed keep their weight."""
        if any(not 0 <= w <= 100 for w in weights.values()):
            raise HTTPException(422, "weights are 0-100")
        with pool.connection() as conn, conn.transaction():
            have = {r["name"]: r["weight"] for r in conn.execute("select name, weight from versions where model = %s for update", (model,))}
            if not have:
                raise HTTPException(404, "no such model, or it has no versions")
            unknown = set(weights) - set(have)
            if unknown:
                raise HTTPException(422, f"not versions of {model}: {sorted(unknown)}")
            after = {**have, **weights}
            if sum(after.values()) == 0:
                raise HTTPException(422, "at least one version needs traffic")
            for name, w in weights.items():
                conn.execute("update versions set weight = %s where name = %s", (w, name))
            rev = bump(conn, f"weights of {model}: {after}")
        return {"model": model, "weights": after, "revision": rev}

    # ---- API keys


    @app.post("/v1/keys", status_code=201, dependencies=[admin])
    def create_key(k: KeyIn):
        secret = "sk-ig-" + secrets.token_urlsafe(24)
        kid = _key_id()
        with pool.connection() as conn, conn.transaction():
            conn.execute(
                "insert into api_keys (id, name, key_hash, rpm, tpm, models) values (%s, %s, %s, %s, %s, %s)",
                (kid, k.name, hash_key(secret), k.rpm, k.tpm, k.models),
            )
            rev = bump(conn, f"key {kid} ({k.name}) created")
        # the only time the secret is shown
        return {"id": kid, "key": secret, "name": k.name, "rpm": k.rpm, "tpm": k.tpm, "revision": rev}

    @app.get("/v1/keys", dependencies=[admin])
    def list_keys():
        with pool.connection() as conn:
            return conn.execute("select id, name, rpm, tpm, models, created_at, revoked_at from api_keys order by created_at").fetchall()

    @app.delete("/v1/keys/{kid}", dependencies=[admin])
    def revoke_key(kid: str):
        with pool.connection() as conn, conn.transaction():
            if not conn.execute("update api_keys set revoked_at = now() where id = %s and revoked_at is null", (kid,)).rowcount:
                raise HTTPException(404, "no such active key")
            rev = bump(conn, f"key {kid} revoked")
        return {"revision": rev}

    return app


app = None
if os.environ.get("IG_REGISTRY_AUTOAPP", "1") == "1":
    app = create_app()
