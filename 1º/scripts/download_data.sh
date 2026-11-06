#!/usr/bin/env bash
# Download NYC TLC Yellow Taxi trip records (Parquet) and the zone lookup table.
# Usage: scripts/download_data.sh [START_YEAR] [END_YEAR] [DEST_DIR]
#   e.g. scripts/download_data.sh 2024 2024 data
set -euo pipefail

START=${1:-2024}
END=${2:-$START}
DEST=${3:-$HOME/cp-proj1-work/data}
BASE="https://d37ci6vzurychx.cloudfront.net"

mkdir -p "$DEST/yellow"
curl -fsSL -o "$DEST/taxi_zone_lookup.csv" "$BASE/misc/taxi_zone_lookup.csv"

for y in $(seq "$START" "$END"); do
  for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
    f="yellow_tripdata_${y}-${m}.parquet"
    out="$DEST/yellow/year=${y}/$f"
    [ -s "$out" ] && continue
    mkdir -p "$DEST/yellow/year=${y}"
    if curl -fsSL -o "$out.tmp" "$BASE/trip-data/$f"; then
      mv "$out.tmp" "$out"; echo "ok   $f"
    else
      rm -f "$out.tmp"; echo "skip $f (not published)"
    fi
  done
done
