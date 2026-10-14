#!/usr/bin/env bash
# Reproduces the headline: 10,000 user changes across GitHub and DevRev, with
# 20% of webhooks delivered twice, 5% never delivered, every delivery delayed
# 0-2 s (so they arrive out of order), and 2% 429s / 1% 5xx from both APIs.
# Needs Go and a local Postgres (DATABASE_ADMIN_URL).
set -euo pipefail
cd "$(dirname "$0")/.."

EVENTS=${EVENTS:-10000}
OBJECTS=${OBJECTS:-1000}
ADMIN_URL=${DATABASE_ADMIN_URL:-postgres://localhost:5432/postgres?sslmode=disable}
DB=devrev_sync_bench_$(date +%s)
OUT=bench/results
mkdir -p bin "$OUT/logs"

go build -o bin/ ./cmd/sync ./cmd/sim ./cmd/chaos
psql "$ADMIN_URL" -qc "CREATE DATABASE $DB"
pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  psql "$ADMIN_URL" -qc "DROP DATABASE IF EXISTS $DB WITH (FORCE)" || true
}
trap cleanup EXIT

export GITHUB_WEBHOOK_SECRET=bench-gh-secret DEVREV_WEBHOOK_SECRET=bench-dv-secret
bin/sim -addr :9100 -sync http://localhost:8090 -dup 0.20 -drop 0.05 -max-delay 2s >"$OUT/logs/sim.log" 2>&1 & pids+=($!)
DATABASE_URL=${ADMIN_URL/\/postgres?//$DB?} GITHUB_REPO=acme/storefront GITHUB_API=http://localhost:9100 GITHUB_TOKEN=sim \
DEVREV_API=http://localhost:9100 DEVREV_TOKEN=sim DEVREV_PART_ID=PROD-1 DEVREV_OWNER_ID=DEVU-1 \
RECONCILE_EVERY=${RECONCILE_EVERY:-5s} WORKERS=32 PORT=8090 \
  bin/sync >"$OUT/logs/sync.log" 2>&1 & pids+=($!)
for _ in $(seq 1 50); do curl -sf localhost:8090/healthz >/dev/null && curl -sf localhost:9100/admin/stats >/dev/null && break; sleep 0.2; done

bin/chaos -events "$EVENTS" -objects "$OBJECTS" -out "$OUT/chaos.json" -md bench/RESULTS.md >/dev/null
cat bench/RESULTS.md
