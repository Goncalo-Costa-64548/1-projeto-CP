#!/usr/bin/env bash
# Scalability study on the Docker standalone cluster.
#   scripts/scale_study.sh [OUTDIR]
# 1. cluster size: 1..MAX_WORKERS workers (WORKER_CORES cores each), full dataset
# 2. dataset scale: fractions on MAX_WORKERS workers
# 3. baseline (--opt none) on MAX_WORKERS workers, for the cluster baseline
set -euo pipefail
OUT=${1:-results/docker}
WORKERS=${WORKERS:-"1 2 3 4"}
MAX_WORKERS=${WORKERS##* }
FRACTIONS=${FRACTIONS:-"0.1 0.2 0.5"}
OPT=${OPT:-all}
cd "$(dirname "$0")/.."
COMPOSE="docker compose -f docker/docker-compose.yml"

scale_to() {
  $COMPOSE up -d --scale spark-worker="$1" >/dev/null 2>&1
  # wait until the master sees every worker as ALIVE
  until [ "$(curl -s localhost:8080/json/ | grep -o '"state" *: *"ALIVE"' | wc -l)" -ge "$1" ]; do sleep 2; done
}

run() {  # run TAG [main.py args...]
  local tag=$1; shift
  scripts/run_docker.sh --years 2024 --tag "$tag" --metrics "$OUT/$tag.json" "$@" 2>&1 | grep "done in" \
    || echo "[$tag] FAILED"
}

for w in $WORKERS; do
  scale_to "$w"
  run "w${w}_f1.0" --opt "$OPT"
done
scale_to "$MAX_WORKERS"
for f in $FRACTIONS; do
  run "w${MAX_WORKERS}_f${f}" --opt "$OPT" --fraction "$f"
done
run "w${MAX_WORKERS}_f1.0_baseline" --opt none --plans "$OUT/plans_baseline"
