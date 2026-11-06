#!/usr/bin/env bash
# Control experiment for the cluster-size study: one worker with as many cores
# as the largest multi-worker run, to separate "more cores" from "more JVMs".
#   scripts/isolate_cores.sh [CORES] [OUTDIR]
set -euo pipefail
CORES=${1:-6}; OUT=${2:-results/docker}
OPT=${OPT:-o1_agg,o2_agg,m3_window,o2_ids}
cd "$(dirname "$0")/.."
COMPOSE="docker compose -f docker/docker-compose.yml"
$COMPOSE down >/dev/null 2>&1
WORKER_CORES=$CORES WORKER_MEMORY=${WORKER_MEMORY:-2g} $COMPOSE up -d --scale spark-worker=1 >/dev/null 2>&1
until [ "$(curl -s localhost:8080/json/ | grep -o '"state" *: *"ALIVE"' | wc -l)" -ge 1 ]; do sleep 2; done
EXEC_MEM=${EXEC_MEM:-1500m} scripts/run_docker.sh --years 2024 --opt "$OPT" --tag "w1c${CORES}_f1.0" \
  --metrics "$OUT/w1c${CORES}_f1.0.json" 2>&1 | grep "done in" || echo "[w1c${CORES}_f1.0] FAILED"
