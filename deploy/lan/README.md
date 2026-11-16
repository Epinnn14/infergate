# LAN mode: a cluster out of the laptops on one Wi-Fi

One computer runs the hub: Redis, PostgreSQL, the registry, the gateway, Prometheus and Grafana.
Every other computer with Docker can join as a model worker with one `docker run`, and any phone
or laptop on the Wi-Fi can chat through the built-in page. This is the same protocol the
Kubernetes deployment uses. Workers only need to reach Redis, so where they run doesn't matter.

## Start the hub

Windows (Docker Desktop running), from the repository root:

```powershell
.\deploy\lan\hub.ps1
```

macOS / Linux: `deploy/lan/hub.sh`

The script creates `deploy/lan/.env` with a random Redis password, API key and admin token (it
is git-ignored). It starts the stack and prints three things:

- the chat page link, `http://<hub-ip>:8080/#key=…`. The key is in the URL fragment, which
  browsers never send to the server. The page stores it and removes it from the address bar.
- the `docker run` command that turns another computer into a worker
- what to check if others can't connect (firewall, network profile, client isolation)

## Join as a worker

Paste the printed command on another computer. It pulls `ghcr.io/epinnn14/infergate-worker`
and connects to the hub's Redis. Within a second the worker appears under **Cluster** on the
chat page, named after that computer. Leave again with `docker rm -f infergate-worker`.

## Things to show

| Do this | What you see |
|---|---|
| Chat a few turns | Every reply shows the worker that answered. A conversation stays on one worker (`affinity`), and its cache hit rate climbs. |
| Press **Burst ×20** | 20 new conversations at once, spread over all computers. The page shows how many each one served. |
| A friend joins | A new card appears in Cluster and the Activity feed says it joined. The next burst uses it too. |
| Close a laptop lid, or `docker rm -f` on it | Its card turns to "left" within a few seconds. Requests it was working on are taken over by the others, and no chat fails. |
| Shift traffic to `sim-v2` | Start a v2 worker (`-e IG_MODEL=sim-v2`), then `PUT /v1/models/chat/weights` with the admin token. The weight bars follow. |

## Security notes

- Redis is open on the network with a password. Anyone who has the join command can act as a
  worker. Use it on networks you trust and stop the hub afterwards (`hub.ps1 -Stop`).
- Create new secrets with `hub.ps1 -NewSecrets`.
- `/status` is public and shows worker names and load only: no keys, prompts or replies.
