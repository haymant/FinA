#!/usr/bin/env python3
"""sonicetl scheduler example — run `examples/scheduler/pipelines.yml` through the
scheduler: stage 1 (market -> in-memory duckdb), stage 2 (instruments unwind +
join, one parquet), stage 3 (fan-out by instrument name over 10 workers, one
parquet per partition).

Run from the `sonicetl` directory after installing the package:

    pip install .
    python examples/scheduler/run.py
"""

from __future__ import annotations

import json
import os

import sonicetl


def main() -> None:
    here = os.path.dirname(os.path.abspath(__file__))

    print("sonicetl scheduler example")
    print("=" * 60)

    run_dir = f"{here}/out"
    os.makedirs(run_dir, exist_ok=True)
    os.chdir(here)  # pipelines.yml uses URIs relative to this directory

    with open(f"{here}/pipelines.yml", encoding="utf-8") as f:
        yaml_text = f.read()

    # 1) show the expansion to scheduler tasks (nothing runs) --------------------
    plan = json.loads(sonicetl.expand_etl_config(yaml_text, retries=2))
    print("\nexpand_etl_config -> task plan")
    for si, stage in enumerate(plan["stages"], 1):
        print(f"  stage {si}: {len(stage)} task(s)")
        for task in stage:
            ctx = task["info"].get("ctx") or {}
            name = task["info"]["job"]["pipeline_yaml"].splitlines()[0].split(": ", 1)[1]
            part = ctx.get("partition")
            units = ctx.get("units") or []
            print(
                f"    {task['id']:<34} {name}"
                f"  partition={part}  units={len(units)}"
            )

    # 2) run the whole thing through the scheduler -------------------------------
    print("\nrun_pipelines_scheduled (workers=10)")
    result = sonicetl.run_pipelines_scheduled(yaml_text, workers=10, retries=2, poll_ms=50)
    print("  ok      :", result.ok)
    print("  rows    :", result.rows)
    print("  timing  :", result.timing)
    print("  slots   :", result.slots)

    print("\n  task summary")
    for t in result.tasks:
        ctx = (t.info or {}).get("ctx") or {}
        part = ctx.get("partition")
        print(f"    {t.id:<38} {t.state:<8} partition={part}")

    # 3) read back the fan-out parquet files --------------------------------------
    fanout = f"{here}/out/fanout"
    files = sorted(f for f in os.listdir(fanout) if f.endswith(".parquet"))
    try:
        import pyarrow.parquet as pq

        print(f"\nfan-out parquet files: {files}")
        total = 0
        for f in files:
            t = pq.read_table(f"{fanout}/{f}")
            total += t.num_rows
            cols = ", ".join(t.column_names)
            print(f"  {f:<36} {t.num_rows} rows  [{cols}]")
        print(f"  total rows: {total}")
    except ImportError:
        print(f"\n[pyarrow not installed; fan-out files: {files}]")


if __name__ == "__main__":
    main()