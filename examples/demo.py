#!/usr/bin/env python3
"""sonicetl demo — build a config programmatically, run a whole ETL, inspect output.

Run from the `sonicetl` directory after installing the package:

    pip install .
    python examples/demo.py
"""

from __future__ import annotations

import os
import pathlib
import tempfile

import sonicetl


CONFIG_YAML = """
pipeline_name: demo_instruments
source:
  file_path: input.json
datasets:
  - name: instrument_master
    type: master
    fields:
      - {name: instrument_id, expression: "coalesce($.instrumentName, $.instrument.name)"}
      - {name: prodType, expression: "coalesce($.KIKOSelect.productType, 'OTC')"}
      - {name: notional, expression: "cast(coalesce($.notional.$numberDecimal, '0') as double)"}
      - {name: family, expression: "coalesce($.instrument.classification.family, 'EQD')"}
  - name: instrument_raw
    type: raw
    fields:
      - {name: instrument_id, expression: "coalesce($.instrumentName, $.instrument.name)"}
      - {name: json_blob, expression: "to_json_string($)"}
  - name: instrument_unwound
    type: unwound
    unwind_rules:
      - {name: unw, condition: "$.instrumentName CONTAINS 'FCN'",
         unwind_path: "$.KIKOSelect.underlying", output_alias: "symbol"}
    fields:
      - {name: instrument_id, expression: "coalesce($.instrumentName, $.instrument.name)"}
      - {name: symbol, expression: "$.symbol"}
      - {name: notional, expression: "cast(coalesce($.notional.$numberDecimal, '0') as double)"}
"""


INPUT_JSON = b"""[
  {"instrumentName":"FCN_BARRIER_001","notional":{"$numberDecimal":"2500000"},
   "KIKOSelect":{"productType":"FCN","underlying":["AAPL","MSFT","QQQ"]},
   "instrument":{"name":"FCN_BARRIER_001","classification":{"family":"EQD"}}},
  {"instrumentName":"BONUS_STRUCTURE","notional":{"$numberDecimal":"1200000"},
   "instrument":{"name":"BONUS_STRUCTURE","classification":{"family":"EQD"}}}
]"""


def main() -> None:
    print("sonicetl demo")
    print("=" * 60)

    # 1) orjson-style JSON codecs -------------------------------------------------
    print("loads  :", sonicetl.loads(INPUT_JSON))
    print("dumps  :", sonicetl.dumps({"foo": [1, 2.5, True, None, "x"]}))

    # 2) programmatic config ------------------------------------------------------
    cfg = sonicetl.ETLConfig(
        pipeline_name="demo_instruments",
        source_file_path="input.json",
        datasets=[
            sonicetl.Dataset(
                "instrument_master",
                "master",
                [sonicetl.FieldSpec("instrument_id", "coalesce($.instrumentName, $.instrument.name)"),
                 sonicetl.FieldSpec("notional", "cast(coalesce($.notional.$numberDecimal, '0') as double)")],
            ),
            sonicetl.Dataset(
                "instrument_unwound",
                "unwound",
                [sonicetl.FieldSpec("symbol", "$.symbol")],
                [sonicetl.UnwindRule("unw", "$.instrumentName CONTAINS 'FCN'",
                                     "$.KIKOSelect.underlying", "symbol")],
            ),
        ],
    )
    print("\nGenerated YAML:\n" + cfg.to_yaml().rstrip() + "\n" + "-" * 60)

    # 3) run the whole ETL ----- --------------------------------------------------
    out = tempfile.mkdtemp(prefix="sonicetl_demo_")
    for label, config in (("from YAML string", CONFIG_YAML), ("from ETLConfig", cfg)):
        print(f"\nrun (config: {label})")
        result = sonicetl.run(config, INPUT_JSON, out_dir=out)
        print("  rows     :", result.rows)
        print("  breakdown:", result.breakdown())
        print("  parquet  :", sorted(os.listdir(out)))

    # 4) read one dataset back ----------------------------------------------------
    # (demonstrate the Parquet output is real; any parquet reader works)
    try:
        import pyarrow.parquet as pq

        for ds in ("instrument_master", "instrument_unwound"):
            t = pq.read_table(f"{out}/{ds}.parquet")
            print(f"\npyarrow read {ds}: {t.num_rows} rows x {t.num_columns} cols")
            print(t.to_pydict())
    except ImportError:
        print("\n[pyarrow not installed; skipping parquet read-back check]")


if __name__ == "__main__":
    main()
