#!/usr/bin/env bash
# Runs the platform as plain processes (no Kubernetes) and compares routing strategies.
#
#   scripts/bench-local.sh            # needs: go, python3 with worker deps, redis-server
#
# Starts Redis, N fake-backend workers and the gateway twice: with prefix-affinity routing
# and with a single shared queue, under the same multi-turn chat load. Results go to results/.
set -euo pipefail
cd "$(dirname "$0")/.."

WORKERS=${WORKERS:-4}
CAPACITY=${CAPACITY:-8}
CACHE=${CACHE:-24}          # prefixes each worker can keep (KV-cache memory is limited)
CONVS=${CONVS:-64}
CONC=${CONC:-24}
DUR=${DUR:-30s}
PORT_REDIS=${PORT_REDIS:-6390}
PY=${PY:-python3}
mkdir -p results bin
go build -o bin/gateway ./cmd/gateway
go build -o bin/loadgen ./cmd/loadgen

cleanup() { kill $(jobs -p) 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT

redis-server --port "$PORT_REDIS" --save "" --appendonly no >/dev/null &
sleep 0.5

cat > results/routes.json <<JSON
{"revision": 1,
 "models": [{"name": "chat", "versions": [{"name": "sim-v1", "weight": 100}]}],
 "keys": []}
JSON

run() { # $1 label, $2 affinity true|false
  redis-cli -p "$PORT_REDIS" flushall >/dev/null
  local wp=()
  for i in $(seq 1 "$WORKERS"); do
    IG_REDIS_URL="redis://127.0.0.1:$PORT_REDIS/0" IG_MODEL=sim-v1 IG_WORKER_ID="w$i" IG_BACKEND=fake \
    IG_CAPACITY="$CAPACITY" IG_FAKE_PREFIX_CACHE="$CACHE" IG_METRICS_PORT=$((9200 + i)) \
    PYTHONPATH=worker "$PY" -m infergate_worker.main >"results/worker-$i.log" 2>&1 &
    wp+=($!)
  done
  IG_REDIS_ADDR="127.0.0.1:$PORT_REDIS" IG_ROUTES_FILE=results/routes.json IG_REQUIRE_KEY=false \
  IG_AFFINITY="$2" IG_LISTEN=:8088 ./bin/gateway >"results/gateway-$1.log" 2>&1 &
  local gp=$!
  sleep 2
  ./bin/loadgen -url http://127.0.0.1:8088 -model chat -c "$CONC" -d "$DUR" -conversations "$CONVS" \
    -label "$1" -json "results/$1.json"
  # cache hit ratio from the workers' metrics
  local hit=0 miss=0
  for i in $(seq 1 "$WORKERS"); do
    m=$(curl -s "http://127.0.0.1:$((9200 + i))/metrics")
    hit=$((hit + $(echo "$m" | awk '/^infergate_worker_prefix_cache_total.*result="hit"/{s+=$2} END{printf "%d", s}')))
    miss=$((miss + $(echo "$m" | awk '/^infergate_worker_prefix_cache_total.*result="miss"/{s+=$2} END{printf "%d", s}')))
  done
  echo "  prefix cache hit rate: $(( 100 * hit / (hit + miss + 1) ))% ($hit hits, $miss misses)"
  echo "{\"hits\": $hit, \"misses\": $miss}" > "results/$1-cache.json"
  kill "$gp" "${wp[@]}"; wait "$gp" "${wp[@]}" 2>/dev/null || true
}

run affinity true
run shared-queue false
