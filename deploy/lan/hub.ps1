<#
.SYNOPSIS
  Start infergate in LAN mode on this computer and print how phones and other laptops on the
  same Wi-Fi can use it or join it as workers.

.EXAMPLE
  .\deploy\lan\hub.ps1                 # one local worker, friends add more
  .\deploy\lan\hub.ps1 -Workers 0      # only the other laptops serve requests
  .\deploy\lan\hub.ps1 -Stop           # stop everything
  .\deploy\lan\hub.ps1 -NewSecrets     # new password, API key and admin token
#>
param(
  [int]$Workers = 1,
  [switch]$Stop,
  [switch]$NewSecrets
)
# Continue, not Stop: docker writes progress to stderr, which Windows PowerShell 5.1 would turn
# into terminating errors. Failures are checked through $LASTEXITCODE instead.
$ErrorActionPreference = "Continue"
$root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$envFile = Join-Path $PSScriptRoot ".env"
$compose = @("compose", "--env-file", $envFile, "-f", "docker-compose.yaml", "-f", "deploy/lan/hub.yaml")

function New-Secret([int]$n) {
  $chars = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
  $bytes = New-Object byte[] $n
  [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
  -join ($bytes | ForEach-Object { $chars[$_ % $chars.Length] })
}

if ($NewSecrets -or -not (Test-Path $envFile)) {
  @(
    "IG_LAN_REDIS_PASSWORD=$(New-Secret 24)",
    "IG_LAN_KEY=sk-lan-$(New-Secret 20)",
    "IG_LAN_ADMIN=$(New-Secret 24)"
  ) | Set-Content -Encoding ascii $envFile
}
$cfg = @{}
Get-Content $envFile | ForEach-Object { if ($_ -match '^(\w+)=(.*)$') { $cfg[$Matches[1]] = $Matches[2] } }

Push-Location $root
try {
  if ($Stop) {
    docker @compose down --remove-orphans
    Write-Host "infergate stopped."
    return
  }
  $env:IG_LAN_HUB_NAME = $env:COMPUTERNAME
  # start clean so the registry is created with the keys in .env
  docker @compose down --remove-orphans | Out-Null
  docker @compose up -d --build --scale worker=$Workers
  if ($LASTEXITCODE -ne 0) { throw "docker compose failed (is Docker Desktop running?)" }
} finally {
  Pop-Location
}

# the Wi-Fi address others reach this computer at
$ip = Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue |
  Where-Object {
    $_.IPAddress -notmatch '^(127\.|169\.254\.)' -and
    $_.InterfaceAlias -notmatch 'vEthernet|WSL|Docker|Loopback|VirtualBox|VMware|Hyper-V'
  } |
  Sort-Object { if ($_.InterfaceAlias -match 'Wi-?Fi|Wireless|WLAN') { 0 } else { 1 } } |
  Select-Object -First 1 -ExpandProperty IPAddress
if (-not $ip) { $ip = "<this-computer-ip>" }

$redisUrl = "redis://:$($cfg.IG_LAN_REDIS_PASSWORD)@${ip}:6379/0"
$image = "ghcr.io/epinnn14/infergate-worker:latest"
$runArgs = "-d --name infergate-worker --restart unless-stopped -e IG_REDIS_URL=$redisUrl -e IG_MODEL=sim-v1 -e IG_DEAD_AFTER_S=6"

Write-Host ""
Write-Host "infergate is running in LAN mode on $ip" -ForegroundColor Green
Write-Host ""
Write-Host "Chat page (open on any phone or laptop on this Wi-Fi):"
Write-Host "  http://${ip}:8080/#key=$($cfg.IG_LAN_KEY)" -ForegroundColor Cyan
Write-Host "Grafana:  http://${ip}:3000      Admin API token: $($cfg.IG_LAN_ADMIN)"
Write-Host ""
Write-Host "Join as a worker from another computer (needs Docker):"
Write-Host "  Windows PowerShell:"
Write-Host ('  docker run ' + $runArgs + ' -e IG_WORKER_ID=$env:COMPUTERNAME ' + $image) -ForegroundColor Cyan
Write-Host "  macOS / Linux:"
Write-Host ('  docker run ' + $runArgs + ' -e IG_WORKER_ID=$(hostname) ' + $image) -ForegroundColor Cyan
Write-Host "  With a real model instead (Ollama installed there, after: ollama pull llama3.2:1b),"
Write-Host "  swap the simulated backend for Ollama; it shows up as model 'llama' on the chat page:"
$ollama = "-e IG_MODEL=ollama-v1 -e IG_BACKEND=openai -e IG_CAPACITY=2 -e IG_UPSTREAM_URL=http://host.docker.internal:11434/v1 -e IG_UPSTREAM_MODEL=llama3.2:1b"
Write-Host ('  docker run ' + ($runArgs -replace '-e IG_MODEL=sim-v1', $ollama) + ' -e IG_WORKER_ID=$env:COMPUTERNAME ' + $image) -ForegroundColor Cyan
Write-Host "  (macOS / Linux: same flags; on Linux add --add-host host.docker.internal:host-gateway)"
Write-Host "  Leave again:  docker rm -f infergate-worker"
Write-Host ""
Write-Host "If others cannot connect:"
Write-Host "  - Windows Firewall: allow Docker Desktop when asked, or in an admin PowerShell run"
Write-Host "    New-NetFirewallRule -DisplayName infergate-lan -Direction Inbound -Protocol TCP -LocalPort 8080,6379,3000 -Action Allow -Profile Private"
Write-Host "    and set this Wi-Fi network to Private in Windows settings."
Write-Host "  - Campus and cafe Wi-Fi often block devices from seeing each other: use a phone hotspot."
Write-Host ""
Write-Host "Stop: .\deploy\lan\hub.ps1 -Stop"
