#!/usr/bin/env bash
# Submit the pipeline to the Docker standalone cluster. Extra args go to src/main.py.
#   docker compose -f docker/docker-compose.yml up -d --scale spark-worker=2
#   scripts/run_docker.sh --years 2024 --tag docker-2w --metrics results/docker/2w.json
# Relative --metrics/--plans paths are resolved inside the project folder.
# One executor per worker (WORKER_CORES cores each); DRIVER_MEM / EXEC_MEM size the JVMs.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker/docker-compose.yml exec -w /opt/project spark-master \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --driver-memory "${DRIVER_MEM:-1g}" --executor-memory "${EXEC_MEM:-1g}" \
  --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///opt/work/spark-events \
  src/main.py --input /opt/work/data/yellow --zones /opt/work/data/taxi_zone_lookup.csv \
  --out /opt/work/output "$@"
