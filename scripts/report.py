#!/usr/bin/env python3
"""Markdown table from loadgen JSON results: scripts/report.py results/affinity.json results/shared-queue.json"""
import json
import os
import sys

print("| Run | OK / total | req/s | tokens/s | TTFT p50 / p90 / p99 ms | latency p50 / p99 ms | routes | prefix cache hits |")
print("|---|---|---|---|---|---|---|---|")
for path in sys.argv[1:]:
    r = json.load(open(path))
    cache = path.replace(".json", "-cache.json")
    hit = ""
    if os.path.exists(cache):
        c = json.load(open(cache))
        hit = f"{100 * c['hits'] / max(1, c['hits'] + c['misses']):.0f}%"
    t, l = r["ttft_ms"], r["latency_ms"]
    routes = ", ".join(f"{k} {v}" for k, v in sorted(r["routes"].items()) if k) or "-"
    print(f"| {r['label']} | {r['ok']} / {r['requests']} | {r['requests_per_s']} | {r['tokens_per_s']} "
          f"| {t['p50']:.0f} / {t['p90']:.0f} / {t['p99']:.0f} | {l['p50']:.0f} / {l['p99']:.0f} | {routes} | {hit} |")
