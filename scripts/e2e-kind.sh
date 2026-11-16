#!/usr/bin/env bash
# End-to-end test on a kind cluster: install KEDA and the chart from local images, then check
#   1. the API works (auth, non-streaming, streaming),
#   2. load makes KEDA scale the workers out,
#   3. shifting 10% of traffic to a canary version that has 0 replicas scales it from zero,
#   4. force-deleting a worker pod mid-load loses no request.
# Needs: kind cluster (current kubectl context), helm, jq, images infergate.local/*:dev loaded,
# bin/loadgen. Writes a Markdown report to $REPORT (GitHub step summary in CI).
set -euo pipefail
cd "$(dirname "$0")/.."

NS=infergate
REPORT=${REPORT:-/dev/stdout}
KEY=sk-demo-local
OUT=results/e2e
mkdir -p "$OUT"
pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

log() { echo "::group::$*"; }
endlog() { echo "::endgroup::"; }
replicas() { kubectl -n $NS get deploy "infergate-worker-$1" -o jsonpath='{.status.readyReplicas}' | sed 's/^$/0/'; }

log "install KEDA"
helm repo add kedacore https://kedacore.github.io/charts >/dev/null
helm upgrade --install keda kedacore/keda --version 2.16.0 -n keda --create-namespace --wait --timeout 5m
endlog

log "install infergate"
helm upgrade --install infergate deploy/helm/infergate -n $NS --create-namespace --wait --timeout 6m \
  -f scripts/e2e-values.yaml
kubectl -n $NS get pods -o wide
endlog

kubectl -n $NS port-forward svc/infergate-gateway 18080:80 >/dev/null 2>&1 & pids+=($!)
kubectl -n $NS port-forward svc/infergate-registry 18090:8090 >/dev/null 2>&1 & pids+=($!)
for _ in $(seq 60); do curl -sf localhost:18080/readyz >/dev/null && break; sleep 1; done
GW=http://localhost:18080
ADMIN=$(kubectl -n $NS get secret infergate -o jsonpath='{.data.admin-token}' | base64 -d)

{
  echo "## infergate end-to-end on kind"
  echo
  echo "| Check | Result |"
  echo "|---|---|"
} >> "$REPORT"
pass() { echo "| $1 | ✅ $2 |" >> "$REPORT"; }
fail() { echo "| $1 | ❌ $2 |" >> "$REPORT"; echo "FAILED: $1: $2" >&2; kubectl -n $NS get pods,scaledobjects,hpa >&2 || true; exit 1; }

# ---------------------------------------------------------------- 1. API
log "smoke"
code=$(curl -s -o /dev/null -w '%{http_code}' $GW/v1/chat/completions -d '{"model":"chat","messages":[{"role":"user","content":"hi"}]}')
[ "$code" = 401 ] || fail "auth" "no key gave $code"
body=$(curl -sf $GW/v1/chat/completions -H "Authorization: Bearer $KEY" \
  -d '{"model":"chat","messages":[{"role":"user","content":"hello"}],"max_tokens":16}')
echo "$body" | jq -e '.choices[0].message.content | length > 0' >/dev/null || fail "completion" "$body"
chunks=$(curl -sfN $GW/v1/chat/completions -H "Authorization: Bearer $KEY" \
  -d '{"model":"chat","messages":[{"role":"user","content":"stream please"}],"stream":true,"max_tokens":16}' | grep -c '^data: ')
[ "$chunks" -gt 3 ] || fail "streaming" "$chunks SSE chunks"
pass "API" "401 without key, completion and SSE stream ($chunks chunks) work"
endlog

# ---------------------------------------------------------------- 2. scale out under load
log "scale out"
before=$(replicas sim-v1)
bin/loadgen -url $GW -key $KEY -c 48 -d 75s -conversations 32 -max-tokens 64 -label scale-out -json $OUT/scale-out.json > $OUT/scale-out.txt &
lg=$!; peak=$before
while kill -0 $lg 2>/dev/null; do r=$(replicas sim-v1); [ "$r" -gt "$peak" ] && peak=$r; sleep 3; done
wait $lg; cat $OUT/scale-out.txt
bad=$(jq '[.codes | to_entries[] | select(.key != "200") | .value] | add // 0' $OUT/scale-out.json)
[ "$peak" -gt "$before" ] || fail "KEDA scale-out" "sim-v1 stayed at $before replicas"
[ "$bad" -eq 0 ] || fail "KEDA scale-out" "$bad non-200 responses"
pass "KEDA scale-out" "sim-v1 workers $before → $peak under 48 concurrent clients; $(jq .ok $OUT/scale-out.json) requests, all 200, TTFT p50 $(jq .ttft_ms.p50 $OUT/scale-out.json) ms"
endlog

# ---------------------------------------------------------------- 3. canary from zero
log "canary"
# KEDA scales the idle canary to zero right after install
for _ in $(seq 30); do [ "$(replicas sim-v2)" = 0 ] && break; sleep 2; done
[ "$(replicas sim-v2)" = 0 ] || fail "canary" "sim-v2 should idle at 0 replicas"
curl -sf -X PUT localhost:18090/v1/models/chat/weights -H "Authorization: Bearer $ADMIN" \
  -H 'Content-Type: application/json' -d '{"sim-v1":80,"sim-v2":20}' | jq -c .
sleep 5   # one registry poll
bin/loadgen -url $GW -key $KEY -c 16 -d 60s -conversations 64 -max-tokens 32 -label canary -json $OUT/canary.json | tee $OUT/canary.txt
v2=$(jq '.versions["sim-v2"] // 0' $OUT/canary.json); total=$(jq .ok $OUT/canary.json)
bad=$(jq '[.codes | to_entries[] | select(.key != "200") | .value] | add // 0' $OUT/canary.json)
[ "$v2" -gt 0 ] || fail "canary" "no request reached sim-v2"
[ "$bad" -eq 0 ] || fail "canary" "$bad non-200 responses"
pass "Canary from zero" "20% weight on sim-v2 (0 replicas) → KEDA started $(replicas sim-v2) worker(s); $v2 of $total requests served by sim-v2, all 200"
endlog

# ---------------------------------------------------------------- 4. chaos
log "chaos"
curl -sf -X PUT localhost:18090/v1/models/chat/weights -H "Authorization: Bearer $ADMIN" \
  -H 'Content-Type: application/json' -d '{"sim-v1":100,"sim-v2":0}' >/dev/null
sleep 5
bin/loadgen -url $GW -key $KEY -c 16 -d 60s -stream=false -conversations 32 -max-tokens 64 -label chaos -json $OUT/chaos.json > $OUT/chaos.txt &
lg=$!
sleep 15
victim=$(kubectl -n $NS get pods -l app.kubernetes.io/component=worker-sim-v1 -o jsonpath='{.items[0].metadata.name}')
kubectl -n $NS delete pod "$victim" --grace-period=0 --force
wait $lg; cat $OUT/chaos.txt
bad=$(jq '[.codes | to_entries[] | select(.key != "200") | .value] | add // 0' $OUT/chaos.json)
[ "$bad" -eq 0 ] || fail "chaos" "$bad requests failed after killing $victim"
pass "Worker killed mid-load" "force-deleted $victim; $(jq .ok $OUT/chaos.json) requests, none lost (latency p99 $(jq .latency_ms.p99 $OUT/chaos.json) ms)"
endlog

kubectl -n $NS get pods >&2
echo >> "$REPORT"
echo "All end-to-end checks passed." >> "$REPORT"
