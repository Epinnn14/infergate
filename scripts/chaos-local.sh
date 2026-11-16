#!/usr/bin/env bash
# Kills a worker with SIGKILL in the middle of a load test and shows what happens to the
# requests it was serving or had queued: they are rescued by the other workers.
set -euo pipefail
cd "$(dirname "$0")/.."
PORT_REDIS=${PORT_REDIS:-6392}
PY=${PY:-python3}
DUR=${DUR:-40s}
STREAM=${STREAM:-false}
mkdir -p results bin
go build -o bin/gateway ./cmd/gateway
go build -o bin/loadgen ./cmd/loadgen
cleanup() { kill $(jobs -p) 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT

redis-server --port "$PORT_REDIS" --save "" --appendonly no >/dev/null &
sleep 0.5
cat > results/routes.json <<JSON
{"revision": 1, "models": [{"name": "chat", "versions": [{"name": "sim-v1", "weight": 100}]}], "keys": []}
JSON
declare -A wpid
for i in 1 2 3; do
  IG_REDIS_URL="redis://127.0.0.1:$PORT_REDIS/0" IG_MODEL=sim-v1 IG_WORKER_ID="w$i" IG_CAPACITY=8 \
  IG_DEAD_AFTER_S=5 IG_RECLAIM_IDLE_S=10 IG_METRICS_PORT=$((9300 + i)) \
  PYTHONPATH=worker "$PY" -m infergate_worker.main >"results/chaos-worker-$i.log" 2>&1 &
  wpid[$i]=$!
done
IG_REDIS_ADDR="127.0.0.1:$PORT_REDIS" IG_ROUTES_FILE=results/routes.json IG_REQUIRE_KEY=false IG_LISTEN=:8087 \
  ./bin/gateway >results/chaos-gateway.log 2>&1 &
sleep 2
( sleep 10; echo ">>> kill -9 worker w2"; kill -9 "${wpid[2]}" ) &
./bin/loadgen -url http://127.0.0.1:8087 -c 24 -d "$DUR" -conversations 64 -stream="$STREAM" -label "chaos (w2 killed at 10s)" -json results/chaos.json
rescued=0
for i in 1 3; do
  rescued=$((rescued + $(curl -s "http://127.0.0.1:$((9300 + i))/metrics" | awk '/^infergate_worker_reclaimed_total/{s+=$2} END{printf "%d", s}')))
done
echo "  requests taken over from the killed worker: $rescued"
