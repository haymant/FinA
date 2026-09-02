#!/usr/bin/env python3
"""fina demo — build a config programmatically, run a set of pipelines, inspect output.

Run from the `fina` directory after installing the package:

    pip install .
    python examples/demo.py
"""

from __future__ import annotations

import os
import tempfile

import fina


# Two pipelines: the first loads a spot reference table into the shared
# in-memory duckdb store; the second unwinds one row per underlying and
# LEFT-JOINs against that table to enrich each row with the live spot.
def build_config(spot_file: str, uni_file: str, out_uri: str) -> fina.PipelinesConfig:
    return fina.PipelinesConfig([
        fina.Pipeline(
            name="mktDataETL",
            sources=[fina.Source("spot", spot_file)],
            datasets=[
                fina.Dataset(
                    "spot", "raw",
                    to=fina.Output("memory://spot"),
                    fields=[
                        fina.Field("name", "$._id"),
                        fina.Field("spot", "cast($.spot as double)"),
                    ],
                ),
            ],
        ),
        fina.Pipeline(
            name="prodETL",
            sources=[fina.Source("uni", uni_file)],
            datasets=[
                fina.Dataset(
                    "products", "unwound", source="uni",
                    to=fina.Output(out_uri, partition_by=["currency"]),
                    unwind_rules=[
                        fina.UnwindRule("u", "$.underlyings[0]", "$.underlyings", "u"),
                    ],
                    join=fina.Join("mkt", "memory://spot", "$.u", "name", ["spot"]),
                    fields=[
                        fina.Field("instrument_id", "coalesce($.id, $.name)"),
                        fina.Field("instrument_name", "$.name"),
                        fina.Field("currency", "coalesce($.currency, 'USD')"),
                        fina.Field("symbol", "$.u"),
                        fina.Field("spot", "cast(mkt.spot as double)"),
                    ],
                ),
            ],
        ),
    ])


def main() -> None:
    here = os.path.dirname(os.path.abspath(__file__))
    spot_file = f"{here}/demo/spot.json"
    uni_file = f"{here}/demo/uni.json"

    print("fina demo")
    print("=" * 60)

    # 1) orjson-style JSON codecs -------------------------------------------------
    print("loads  :", fina.loads(open(f"{here}/demo/spot.json", "rb").read())[0])
    print("dumps  :", fina.dumps({"foo": [1, 2.5, True, None, "x"]}))

    # 2) programmatic config ------------------------------------------------------
    cfg = build_config(spot_file, uni_file, "memory://out.products")
    print("\nGenerated YAML:\n" + cfg.to_yaml().rstrip() + "\n" + "-" * 60)

    # 3) run the pipelines from a dict and from the YAML string --------------------
    out = tempfile.mkdtemp(prefix="fina_demo_")
    out_uri = f"file://{out}/products"
    cfg = build_config(spot_file, uni_file, out_uri)
    yml = cfg.to_yaml()

    for label, config in (("from YAML string", yml), ("from PipelinesConfig", cfg)):
        print(f"\nrun_pipelines (config: {label})")
        result = fina.run_pipelines(config)
        print("  rows     :", result.rows)
        print("  breakdown:", result.breakdown())

    # 4) read one dataset back -----------------------------------------------------
    # (demonstrate the Parquet output is real; any parquet reader works)
    try:
        import pyarrow.parquet as pq

        files = sorted(
            os.path.join(dp, f)
            for dp, _, fns in os.walk(out)
            for f in fns
            if f.endswith(".parquet")
        )
        print(f"\nparquet files: {files}")
        for f in files:
            t = pq.read_table(f)
            print(f"pyarrow read {f}: {t.num_rows} rows x {t.num_columns} cols")
            print(t.to_pydict())
    except ImportError:
        print("\n[pyarrow not installed; skipping parquet read-back check]")


if __name__ == "__main__":
    main()