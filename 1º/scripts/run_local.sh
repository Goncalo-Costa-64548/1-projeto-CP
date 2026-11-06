#!/usr/bin/env bash
# Run the pipeline in local mode. Extra args go to src/main.py (and override the defaults below).
#   scripts/run_local.sh --years 2024 --opt none --metrics results/local/base.json
#
# Environment:
#   VENV        virtualenv with pyspark; its path must not contain ':' (breaks PATH/JVM classpath)
#   WORK        data, outputs and event logs; keep it outside folders synced by iCloud/Dropbox,
#               whose upload daemons otherwise compete for CPU and disturb the measurements
#   CORES       local[CORES] (default *)    DRIVER_MEM  driver heap (default 3g)
#   SPARK_CONF  extra spark-submit options, e.g. "--conf spark.sql.adaptive.enabled=false"
set -euo pipefail
VENV=${VENV:-$HOME/.venvs/cp-proj1}
WORK=${WORK:-$HOME/cp-proj1-work}
export PATH="$VENV/bin:$PATH" PYSPARK_PYTHON="$VENV/bin/python" SPARK_LOCAL_IP=127.0.0.1
cd "$(dirname "$0")/.."
mkdir -p "$WORK/spark-events"
spark-submit --master "local[${CORES:-*}]" --driver-memory "${DRIVER_MEM:-3g}" \
  --conf spark.eventLog.enabled=true --conf "spark.eventLog.dir=file://$WORK/spark-events" \
  ${SPARK_CONF:-} \
  src/main.py --input "$WORK/data/yellow" --zones "$WORK/data/taxi_zone_lookup.csv" \
  --out "$WORK/output" "$@"
