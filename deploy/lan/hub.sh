#!/usr/bin/env bash
# Start infergate in LAN mode on this computer (macOS / Linux) and print how phones and other
# laptops on the same Wi-Fi can use it or join as workers. Windows: deploy/lan/hub.ps1.
#   deploy/lan/hub.sh [workers]     # default 1 local worker
#   deploy/lan/hub.sh stop
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
cd "$here/../.."
env_file="$here/.env"
compose=(docker compose --env-file "$env_file" -f docker-compose.yaml -f deploy/lan/hub.yaml)

secret() { LC_ALL=C tr -dc 'abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789' </dev/urandom | head -c "$1"; }
if [ ! -f "$env_file" ]; then
  printf 'IG_LAN_REDIS_PASSWORD=%s\nIG_LAN_KEY=sk-lan-%s\nIG_LAN_ADMIN=%s\n' "$(secret 24)" "$(secret 20)" "$(secret 24)" > "$env_file"
fi
# shellcheck disable=SC1090
. "$env_file"

if [ "${1:-}" = stop ]; then "${compose[@]}" down --remove-orphans; exit 0; fi
export IG_LAN_HUB_NAME="$(hostname -s 2>/dev/null || hostname)"
"${compose[@]}" down --remove-orphans >/dev/null 2>&1 || true
"${compose[@]}" up -d --build --scale "worker=${1:-1}"

ip=$(ipconfig getifaddr en0 2>/dev/null || hostname -I 2>/dev/null | awk '{print $1}' || true)
ip=${ip:-<this-computer-ip>}
run="-d --name infergate-worker --restart unless-stopped -e IG_REDIS_URL=redis://:${IG_LAN_REDIS_PASSWORD}@${ip}:6379/0 -e IG_MODEL=sim-v1 -e IG_DEAD_AFTER_S=6"
image=ghcr.io/epinnn14/infergate-worker:latest
cat <<MSG

infergate is running in LAN mode on $ip

Chat page (open on any phone or laptop on this Wi-Fi):
  http://$ip:8080/#key=$IG_LAN_KEY
Grafana:  http://$ip:3000      Admin API token: $IG_LAN_ADMIN

Join as a worker from another computer (needs Docker):
  Windows PowerShell:
  docker run $run -e IG_WORKER_ID=\$env:COMPUTERNAME $image
  macOS / Linux:
  docker run $run -e IG_WORKER_ID=\$(hostname) $image
  Leave again:  docker rm -f infergate-worker

Campus and cafe Wi-Fi often block devices from seeing each other: use a phone hotspot.
Stop: deploy/lan/hub.sh stop
MSG
