# Using infergate

infergate sits between your apps and your model servers. Apps talk to it like the OpenAI API.
It handles API keys, rate limits, queueing, routing to the server that has the conversation
cached, canary rollouts, autoscaling and metrics. The model itself can be a simulator (for
trying things out), Ollama on a laptop, vLLM on a GPU node, or any OpenAI-compatible API.

Contents:

1. [Get your own copy](#1-get-your-own-copy)
2. [Run it in two minutes](#2-run-it-in-two-minutes)
3. [Connect a real model](#3-connect-a-real-model)
4. [Call it from your app](#4-call-it-from-your-app)
5. [Models, versions, keys and canaries](#5-models-versions-keys-and-canaries)
6. [Deploy it](#6-deploy-it)
7. [Configuration reference](#7-configuration-reference)
8. [Before production](#8-before-production)
9. [Troubleshooting](#9-troubleshooting)

## 1. Get your own copy

**From the template (recommended).** On the repository page, click **Use this template → Create
a new repository**. On the first push, the `template-init` workflow rewrites the Go module path,
the image names (`ghcr.io/<you>/infergate-*`) and links to your repository and commits the
change. Run `git pull` once it has finished. CI then builds and tests everything and publishes
your images to GHCR on every push to `main`.

**From a clone or fork:**

```bash
git clone https://github.com/Epinnn14/infergate.git my-gateway && cd my-gateway
scripts/use-template.sh <your-github-user-or-org> [repo-name]
```

Requirements: Docker for the quick start. Go 1.24 and Python 3.11+ only if you want to run the
tests or change the code. kind, Helm or Terraform only for Kubernetes.

## 2. Run it in two minutes

```bash
docker compose up --build -d --scale worker=3
```

| Open | What |
|---|---|
| http://localhost:8080 | Chat page. Paste the key `sk-demo-local`. Every reply shows which worker answered. |
| http://localhost:8080/v1/chat/completions | The OpenAI-compatible API |
| http://localhost:8090/docs | Admin API with interactive docs, token `admin-local` |
| http://localhost:3000 | Grafana dashboard "infergate" |

The workers simulate a GPU, so replies are filler text. Section 3 swaps in a real model.
Stop with `docker compose down`.

Load test from inside the stack:

```bash
docker compose run --rm --entrypoint /loadgen gateway -url http://gateway:8080 -key sk-demo-local -c 16 -d 30s
```

## 3. Connect a real model

A worker with `IG_BACKEND=openai` sends generation to any OpenAI-compatible server and keeps
everything else: queueing, affinity, rescue, cancellation, metrics.

### Ollama on your computer

```bash
ollama pull llama3.2:1b
docker compose down
docker compose -f docker-compose.yaml -f deploy/compose/ollama.yaml up --build -d --scale worker=1
```

Pick model **llama** on the chat page. Any other OpenAI-compatible engine works the same way:

```bash
OLLAMA_URL=http://host.docker.internal:1234/v1 OLLAMA_MODEL=qwen2.5-7b-instruct \
  docker compose -f docker-compose.yaml -f deploy/compose/ollama.yaml up -d   # LM Studio
```

### Several laptops on one Wi-Fi

One laptop is the hub, and the others join with a single `docker run`, either simulated or
running Ollama. See [deploy/lan](../deploy/lan/README.md).

### vLLM on Kubernetes GPU nodes

[`examples/vllm-gpu.yaml`](../deploy/helm/infergate/examples/vllm-gpu.yaml) runs vLLM as a sidecar
in each worker pod, with prefix caching on. KEDA adds pods (and so GPUs) when requests queue up.

```bash
helm install infergate deploy/helm/infergate -n infergate --create-namespace \
  -f deploy/helm/infergate/examples/vllm-gpu.yaml
```

### An engine that already runs elsewhere, or a hosted API

[`examples/external-engine.yaml`](../deploy/helm/infergate/examples/external-engine.yaml) points
the workers at a URL, with the API key taken from a Secret.

### In-process Hugging Face model

`IG_BACKEND=hf` with `IG_HF_MODEL=Qwen/Qwen2.5-0.5B-Instruct` runs the model inside the worker.
The image must be built with `--build-arg EXTRAS=hf`. This is fine for small models on CPU. For
anything bigger, use vLLM.

### Your own backend

A backend is a class with `start()`, `generate(job, emit, cancelled)` and `stats()`. See
`worker/infergate_worker/backends/`, and register it in `backends/__init__.py`.

## 4. Call it from your app

Any OpenAI client works. Change the base URL and the key.

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8080/v1", api_key="sk-demo-local")
stream = client.chat.completions.create(
    model="chat",                      # a model name from the registry
    messages=[{"role": "system", "content": "You are a helpful assistant."},
              {"role": "user", "content": "Hello!"}],
    stream=True,
)
for chunk in stream:
    print(chunk.choices[0].delta.content or "", end="", flush=True)
```

```javascript
import OpenAI from "openai";
const client = new OpenAI({ baseURL: "http://localhost:8080/v1", apiKey: "sk-demo-local" });
const res = await client.chat.completions.create({ model: "chat", messages: [{ role: "user", content: "Hello!" }] });
console.log(res.choices[0].message.content);
```

Response headers tell you how each request was served: `X-Infergate-Version`,
`X-Infergate-Worker` and `X-Infergate-Route` (`affinity`, `spill` or `shared`).

To get the most out of prefix caching, keep a stable system prompt at the start of your
messages and send the whole history each turn. infergate routes on that stable beginning, so
a conversation stays on the worker that has it cached.

Errors follow OpenAI's shape:

| Code | Meaning |
|---|---|
| 401 | Missing or unknown key |
| 403 | The key is not allowed this model |
| 404 | Unknown model |
| 429 | Rate limit (`Retry-After` header) |
| 503 | Queue full |
| 504 | Request timed out |
| 502 | The model server failed |

## 5. Models, versions, keys and canaries

A **model** is the name apps ask for (`chat`). A **version** is a deployment that serves it
(`sim-v1`, `qwen7b-v2`), with its own workers, queue and autoscaler. The registry decides how
traffic splits across versions. All calls below need `Authorization: Bearer <admin token>`.

| Call | Does |
|---|---|
| `GET /v1/models` | list models, versions and weights |
| `POST /v1/models` `{"name": "chat"}` | add a model |
| `POST /v1/models/chat/versions` `{"name": "chat-v2", "backend": "openai", "weight": 0}` | add a version, without traffic yet |
| `PUT /v1/models/chat/weights` `{"chat-v1": 90, "chat-v2": 10}` | shift traffic; gateways follow within one poll (2–5 s) |
| `POST /v1/keys` `{"name": "team-a", "rpm": 120, "tpm": 200000, "models": ["chat"]}` | create an API key; the secret is shown once |
| `GET /v1/keys`, `DELETE /v1/keys/<id>` | list and revoke keys |

**A canary rollout:**

1. Add the version with weight 0.
2. Deploy its workers, by adding it to `models[].versions` in Helm or starting a worker with
   `IG_MODEL=<version>`.
3. Set the weights to 95/5. Watch "Traffic split by version", TTFT and 5xx in Grafana.
4. Then 50/50, then 0/100.
5. Remove the old version.

A conversation stays on one version while the weights don't change. With KEDA, a version at
0 replicas starts as soon as it gets weight.

## 6. Deploy it

| Where | How |
|---|---|
| One machine | `docker compose up` (section 2) |
| Laptops on one Wi-Fi | [deploy/lan](../deploy/lan/README.md) |
| Local Kubernetes | `make e2e` (kind, KEDA, the chart and the end-to-end checks) or `deploy/terraform/envs/kind` |
| Your cluster | `helm install` with your values, or `deploy/terraform/envs/existing-cluster` (it can also install kube-prometheus-stack, KEDA, Istio and Argo CD) |
| GitOps | `deploy/argocd/application.yaml`, pointed at your repository |

The chart's main switches:

| Value | Default | |
|---|---|---|
| `models` | `chat` with `sim-v1` / `sim-v2` | versions, backend, capacity, autoscaling, `upstream`, `sidecars` |
| `apiKeys` | one demo key | keys created on first install; remove for real deployments |
| `redis.enabled`, `postgres.enabled` | in-cluster | set `false` and fill `external.*` for managed services |
| `keda.enabled` | `true` | needs KEDA; otherwise fixed `replicas` |
| `monitoring.serviceMonitor/prometheusRule/dashboard` | dashboard only | needs the Prometheus Operator / Grafana sidecar |
| `istio.enabled` | `false` | sidecars, mTLS, ingress route |
| `networkPolicy.enabled` | `true` | only the gateway is reachable from outside |

## 7. Configuration reference

**Gateway** (environment):

| Variable | Default | |
|---|---|---|
| `IG_LISTEN` | `:8080` | |
| `IG_REDIS_ADDR`, `IG_REDIS_PASSWORD`, `IG_REDIS_DB` | `127.0.0.1:6379`, empty, `0` | |
| `IG_REGISTRY_URL`, `IG_REGISTRY_TOKEN`, `IG_REGISTRY_INTERVAL` | `http://127.0.0.1:8090`, empty, `5s` | or `IG_ROUTES_FILE` for a static JSON file |
| `IG_REQUIRE_KEY` | `true` | |
| `IG_REQUEST_TIMEOUT` | `2m` | queueing plus generation |
| `IG_MAX_QUEUE` | `1000` | waiting requests per version before 503 |
| `IG_MAX_LOAD` | `1` | above this load a worker gets no more affinity traffic |
| `IG_AFFINITY` | `true` | `false` sends everything to the shared queue |
| `IG_HEARTBEAT_MAX_AGE` | `5s` | |
| `IG_DEFAULT_MAX_TOKENS` | `256` | |

**Worker** (environment):

| Variable | Default | |
|---|---|---|
| `IG_REDIS_URL` | `redis://127.0.0.1:6379/0` | |
| `IG_MODEL` | | the **version** this worker serves |
| `IG_WORKER_ID`, `IG_WORKER_PREFIX` | hostname | the name shown on the chat page and in headers |
| `IG_BACKEND` | `fake` | `fake`, `openai`, `hf` |
| `IG_CAPACITY` | `8` | requests at once; match the engine's concurrency |
| `IG_UPSTREAM_URL`, `IG_UPSTREAM_MODEL`, `IG_UPSTREAM_KEY` | Ollama defaults | `openai` backend |
| `IG_UPSTREAM_WAIT_S` | `900` | how long to wait for the engine to start before joining |
| `IG_DEAD_AFTER_S` | `15` | silence after which peers take over a worker's requests |
| `IG_RECLAIM_IDLE_S` | `60` | stalled shared-queue requests are retried after this |
| `IG_SHUTDOWN_GRACE_S` | `25` | time to finish in-flight work on SIGTERM |
| `IG_METRICS_PORT` | `9100` | |

**Registry**: `IG_DATABASE_URL`, `IG_ADMIN_TOKEN`, `IG_ROUTES_TOKEN`, `IG_BOOTSTRAP` (JSON with
models and keys, applied once on an empty database).

## 8. Before production

- [ ] Remove the demo key from `apiKeys` and create keys with `POST /v1/keys`.
- [ ] Use managed Redis with replication (`redis.external`) and managed PostgreSQL
      (`postgres.external`), or turn on persistence.
- [ ] Keep the admin and routes tokens in an existing Secret (`registry.existingSecret`).
- [ ] Put TLS in front of the gateway (Ingress or the Istio route). Keep the admin API internal.
- [ ] Size `capacity` to the engine, and set `lagPerReplica` to how many waiting requests one
      more replica should absorb.
- [ ] Install the Prometheus Operator and enable the ServiceMonitor and PrometheusRule, then
      route the alerts somewhere.
- [ ] Set per-key `rpm` / `tpm`. They are enforced across all gateway replicas.

## 9. Troubleshooting

| Symptom | Look at |
|---|---|
| 401 on every request | Key typo, or the registry was created with other keys. The bootstrap only runs on an empty database, so `docker compose down` and start again. |
| 504 and requests stuck in the queue | No live worker for that version (chat page or `/status`). Check that `IG_MODEL` on the workers matches the version name. |
| Worker never shows up (openai backend) | Its log says `waiting for engine at …`. Is the engine running, and is the URL reachable from the container? From Docker, use `host.docker.internal`, not `localhost`. |
| Workers don't scale on Kubernetes | `kubectl get scaledobject`: READY must be True. The KEDA operator must reach Redis (NetworkPolicy `kedaNamespace`). |
| A conversation hops between workers | Each turn must start with the same system prompt or first message. Changing the system prompt starts a new routing key. |
| Other laptops can't join in LAN mode | Windows Firewall, network profile set to Public, or Wi-Fi client isolation. See deploy/lan. |
