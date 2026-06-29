#!/usr/bin/env bash
# locus_step_sweep.sh — build a local replica index at each locus_step and run
# the benchmark replay, writing one CSV per step.
#
# Prereqs: local MySQL up (docker-compose.local.yml), .bioindex pointing at it,
# AWS creds with read on the bench S3 prefix, bioindex installed (pip install -e .).
#
# Usage:
#   ./scripts/locus_step_sweep.sh <bench-prefix/> <varids-file> [steps...]
#   ./scripts/locus_step_sweep.sh associations/variant-bench/ varids.txt 20000 500 250 100
set -euo pipefail
cd "$(dirname "$0")/.."

PREFIX="${1:?usage: $0 <bench-prefix/> <varids-file> [steps...]}"
VARIDS="${2:?usage: $0 <bench-prefix/> <varids-file> [steps...]}"
shift 2
STEPS=("${@:-20000 500 250 100}")

INDEX="varbench"
TABLE="VarBench"
OUT="bench-results"
mkdir -p "$OUT"

for STEP in ${STEPS[@]}; do
  SCHEMA="varId=\$chr:\$pos;locus_step=${STEP}"
  echo "==> [step ${STEP}] create + rebuild ${INDEX} over ${PREFIX}"
  yes | python -m bioindex.main create "$INDEX" "$TABLE" "$PREFIX" "$SCHEMA"
  yes | python -m bioindex.main index "$INDEX" --rebuild
  echo "==> [step ${STEP}] replay -> ${OUT}/step-${STEP}.csv"
  python scripts/bench_locus_step.py "$INDEX" "$VARIDS" > "${OUT}/step-${STEP}.csv"
done

echo "==> summary (mean bytes_read, mean latency_ms per step)"
python3 - "$OUT" <<'PY'
import csv, glob, os, statistics, sys
for f in sorted(glob.glob(os.path.join(sys.argv[1], "step-*.csv"))):
    rows = list(csv.DictReader(open(f)))
    if not rows:
        continue
    br = [int(r['bytes_read']) for r in rows]
    lat = [float(r['latency_ms']) for r in rows]
    print(f"{os.path.basename(f):20} n={len(rows):5} "
          f"mean_bytes={statistics.mean(br):14,.0f} "
          f"mean_ms={statistics.mean(lat):8.1f}")
PY
