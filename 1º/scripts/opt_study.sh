#!/usr/bin/env bash
# Cumulative optimisation study: each configuration adds one optimisation.
# Repetitions are interleaved (round-robin over configurations) so that drift in
# machine state (thermal throttling, background load) spreads over all configs.
#   scripts/opt_study.sh [REPS] [YEARS] [OUTDIR] [CONFIGS...]
set -euo pipefail
REPS=${1:-3}; YEARS=${2:-2024}; OUT=${3:-results/local/study}
shift $(( $# < 3 ? $# : 3 ))
CONFIGS=("$@")
[ ${#CONFIGS[@]} -eq 0 ] && CONFIGS=(none o1_agg o1_agg,o2_agg o1_agg,o2_agg,m3_window o1_agg,o2_agg,m3_window,cache)
cd "$(dirname "$0")/.."
for r in $(seq 1 "$REPS"); do
  for cfg in "${CONFIGS[@]}"; do
    name=${cfg//,/+}
    plans=""; [ "$r" = 1 ] && plans="--plans $OUT/plans/$name"
    scripts/run_local.sh --years "$YEARS" --opt "$cfg" --tag "$name" \
      --metrics "$OUT/$name.r$r.json" $plans 2>&1 | grep "done in"
  done
done
