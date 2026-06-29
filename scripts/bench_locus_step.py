#!/usr/bin/env python3
"""bench_locus_step.py — replay varId point lookups against an index and emit
per-query bgzip bytes_read / latency as CSV.

Usage:
  python -m bioindex.main ... is NOT used; this drives query.fetch directly.
  BIOINDEX_* env (or .bioindex) must point at the target DB + S3 bucket.

  python scripts/bench_locus_step.py <index-name> <varids-file>

  <varids-file>  one varId per line (e.g. "1:45123:A:G").

Output (stdout, CSV):
  index,locus_step,varId,bytes_read,kept,filtered,latency_ms
"""
import csv
import sys
import time

import dotenv

from bioindex.lib import config as config_mod
from bioindex.lib import migrate, query
from bioindex.lib import index as index_mod


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: bench_locus_step.py <index-name> <varids-file>")
    index_name, varids_file = sys.argv[1], sys.argv[2]

    dotenv.load_dotenv('.bioindex')
    cfg = config_mod.Config()
    engine = migrate.migrate(cfg)
    idx = index_mod.Index.lookup(engine, index_name, 1)
    step = idx.schema.locus_step

    w = csv.writer(sys.stdout)
    w.writerow(['index', 'locus_step', 'varId', 'bytes_read', 'kept', 'filtered', 'latency_ms'])

    with open(varids_file) as fh:
        for line in fh:
            vid = line.strip()
            if not vid:
                continue
            t0 = time.perf_counter()
            reader = query.fetch(cfg, engine, idx, (vid,))
            for _ in reader.records:   # drain so bytes_read reflects the full read
                pass
            dt_ms = (time.perf_counter() - t0) * 1000.0
            w.writerow([index_name, step, vid, reader.bytes_read,
                        reader.count, reader.filtered_count, f'{dt_ms:.1f}'])
            sys.stdout.flush()


if __name__ == '__main__':
    main()
