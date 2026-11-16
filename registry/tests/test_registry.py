import hashlib
import json

from fastapi.testclient import TestClient

from infergate_registry.app import create_app

ADMIN = {"Authorization": "Bearer admin-t"}
READER = {"Authorization": "Bearer routes-t"}


def client(dsn, monkeypatch=None, bootstrap=None):
    if bootstrap and monkeypatch:
        monkeypatch.setenv("IG_BOOTSTRAP", json.dumps(bootstrap))
    return TestClient(create_app(dsn, admin_token="admin-t", routes_token="routes-t"))


def test_canary_rollout_flow(db):
    with client(db) as c:
        assert c.post("/v1/models", json={"name": "chat"}).status_code == 401
        assert c.post("/v1/models", json={"name": "chat", "description": "general chat"}, headers=ADMIN).status_code == 201
        assert c.post("/v1/models", json={"name": "chat"}, headers=ADMIN).status_code == 409
        assert c.post("/v1/models", json={"name": "Bad Name!"}, headers=ADMIN).status_code == 422
        assert c.post("/v1/models/chat/versions", json={"name": "qwen-v1", "weight": 100}, headers=ADMIN).status_code == 201
        assert c.post("/v1/models/chat/versions", json={"name": "qwen-v2"}, headers=ADMIN).status_code == 201
        assert c.post("/v1/models/nope/versions", json={"name": "x"}, headers=ADMIN).status_code == 404

        r1 = c.get("/v1/routes", headers=READER).json()
        assert r1["models"] == [{"name": "chat", "versions": [{"name": "qwen-v1", "weight": 100}, {"name": "qwen-v2", "weight": 0}]}]

        res = c.put("/v1/models/chat/weights", json={"qwen-v1": 90, "qwen-v2": 10}, headers=ADMIN)
        assert res.status_code == 200 and res.json()["weights"] == {"qwen-v1": 90, "qwen-v2": 10}
        r2 = c.get("/v1/routes", headers=READER).json()
        assert r2["revision"] > r1["revision"]
        assert {v["name"]: v["weight"] for v in r2["models"][0]["versions"]} == {"qwen-v1": 90, "qwen-v2": 10}

        assert c.put("/v1/models/chat/weights", json={"qwen-v1": 0, "qwen-v2": 0}, headers=ADMIN).status_code == 422
        assert c.put("/v1/models/chat/weights", json={"other": 5}, headers=ADMIN).status_code == 422
        assert c.get("/v1/routes").status_code == 401


def test_keys(db):
    with client(db) as c:
        k = c.post("/v1/keys", json={"name": "team-a", "rpm": 600, "tpm": 50000, "models": ["chat"]}, headers=ADMIN).json()
        assert k["key"].startswith("sk-ig-")
        routes = c.get("/v1/routes", headers=READER).json()
        (rk,) = routes["keys"]
        assert rk["hash"] == hashlib.sha256(k["key"].encode()).hexdigest()
        assert rk["rpm"] == 600 and rk["models"] == ["chat"] and "key" not in rk
        assert c.delete(f"/v1/keys/{k['id']}", headers=ADMIN).status_code == 200
        assert c.get("/v1/routes", headers=READER).json()["keys"] == []
        assert c.delete(f"/v1/keys/{k['id']}", headers=ADMIN).status_code == 404


def test_bootstrap_and_migrations_are_idempotent(db, monkeypatch):
    spec = {
        "models": [{"name": "chat", "versions": [{"name": "tiny-v1", "weight": 100}]}],
        "keys": [{"name": "demo", "key": "sk-demo", "rpm": 0}],
    }
    with client(db, monkeypatch, spec) as c:
        r = c.get("/v1/routes", headers=READER).json()
        assert r["models"][0]["versions"] == [{"name": "tiny-v1", "weight": 100}]
        assert r["keys"][0]["hash"] == hashlib.sha256(b"sk-demo").hexdigest()
    # a second start (another replica, a restart) changes nothing
    with client(db, monkeypatch, spec) as c:
        r2 = c.get("/v1/routes", headers=READER).json()
        assert r2["revision"] == r["revision"] and len(r2["keys"]) == 1
        assert c.get("/healthz").json() == {"ok": True}
