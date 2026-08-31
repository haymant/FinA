#!/usr/bin/env python3
"""Benchmark sonicetl on large JSON, mirroring the 3 GB experiment.

Measures two things on the 3 GB `data/uni.json` corpus:

1. **Whole ETL** — `sonicetl.run_pipelines(config)` streaming native parse +
   field extraction + Parquet write, plus peak RSS.
2. **Pure parse (reference)** — materializing the DOM with `sonicetl.loads`
   (sonic-rs), `orjson.loads`, and stdlib `json.loads`, reported for context.

Usage:
    python examples/bench_3gb.py \
        --config ../data/pipelines.yml \
        --input  ../data/uni.json \
        --out    /tmp/pq_sonic

The config may use `{INPUT}` / `{OUT_DIR}` placeholders; they are substituted
from `--input` and `--out`.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import time
from pathlib import Path

import sonicetl


def _peak_rss_kb() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def bench_whole_etl(config: str, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    base_peak = _peak_rss_kb()
    t0 = time.perf_counter()
    result = sonicetl.run_pipelines(config)
    wall = (time.perf_counter() - t0) * 1000.0
    peak = _peak_rss_kb()
    print(f"\nWhole ETL via sonicetl.run_pipelines (native sonic-rs)")
    print(f"  rows              : {result.rows}")
    print(f"  breakdown         : {result.breakdown()}")
    print(f"  ETL total (per-dataset timings): {result.total_etl_ms():9.1f} ms")
    print(f"  wall clock        : {wall:9.1f} ms")
    print(f"  peak RSS          : {(peak - base_peak) / 1024 / 1024:.2f} GB "
          f"(process max {peak / 1024 / 1024:.2f} GB)")


def bench_pure_parse(label: str, fn, data: bytes) -> float:
    # warm-up / verify on a small valid doc
    fn(b'{"warmup": [1, 2, 3]}')
    t0 = time.perf_counter()
    fn(data)
    wall = (time.perf_counter() - t0) * 1000.0
    print(f"  {label:<16} parse+materialize DOM : {wall:9.1f} ms")
    return wall


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default=os.path.join("..", "data", "pipelines.yml"))
    ap.add_argument("--input", default=os.path.join("..", "data", "uni.json"))
    ap.add_argument("--out", default=os.path.join("..", "dist", "sonic"))
    ap.add_argument("--skip-orjson", action="store_true")
    args = ap.parse_args()

    yaml_text = Path(args.config).read_text()
    data = Path(args.input).read_bytes()
    yaml_text = (
        yaml_text.replace("{INPUT}", args.input)
        .replace("{OUT_DIR}", args.out)
    )
    print(f"input : {args.input} ({len(data) / 1e6:.0f} MB, read took "
          f"{resource.getrusage(resource.RUSAGE_SELF).ru_maxrss:.0f}kb baseline)")

    bench_whole_etl(yaml_text, args.out)

    print("\nPure parse (materializing whole DOM — reference only):")
    bench_pure_parse("sonicetl.loads", sonicetl.loads, data)
    if not args.skip_orjson:
        import orjson

        bench_pure_parse("orjson.loads", orjson.loads, data)
    bench_pure_parse("json.loads", json.loads, data)

    print("\nSummary: whole-ETL timings above (disk read excluded, identical "
          "for all paths).")


if __name__ == "__main__":
    main()