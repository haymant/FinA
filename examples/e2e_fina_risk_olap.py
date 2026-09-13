#!/usr/bin/env python3
"""End-to-end coordinator run: fina-core-scheduler → fina-risk → Parquet → OLAP.

Drives the ``FinaProcess`` ``fina-risk-augment-to-olap`` (ETL augment → compile →
fina-risk risk_batch → fina-olap query) with REAL fina-risk compute, persisting
``risk_wide``/``risk_long`` to the shared store, then reads it back.

Run in an environment that has BOTH ``fina-core`` (built) and ``fina-risk``::

    PYTHONPATH=python python examples/e2e_fina_risk_olap.py --count 30 --paths 128 --root /tmp/fina-risk-e2e

The scheduler is transport-neutral: swap ``risk_callable`` for an MCP client
call to the fina-risk ``run_risk_task`` / ``run_etl_task`` tools to run the
compute in a separate process/host.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import yaml


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--process", default="examples/processes/fina-risk-augment-to-olap.yml")
    parser.add_argument("--count", type=int, default=30)
    parser.add_argument("--paths", type=int, default=128)
    parser.add_argument("--seed", type=int, default=20260909)
    parser.add_argument("--root", default="/tmp/fina-risk-e2e")
    args = parser.parse_args()

    from fina_core import SchedulerService, register_fina_handlers, render_parameters
    from fina_risk import scheduler_adapter
    from fina_risk.olap import query_ssrm
    from fina_risk.storage import reload_storage_config

    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)
    os.environ["FINA_OLAP_STORE"] = "local"
    os.environ["FINA_OLAP_PARQUET_ROOT"] = str(root)
    reload_storage_config()

    def olap_callable(payload: dict[str, Any]) -> dict[str, Any]:
        query = payload.get("query") or {"dataset": "risk_wide", "startRow": 0, "endRow": 5}
        result = query_ssrm(query)
        result["rows"] = result.get("rows", [])[:5]
        return result

    class Trades:
        trades: dict[str, Any] = {}

        def register(self, trade: dict[str, Any]) -> dict[str, Any]:
            self.trades[trade.get("trade_id", "trade")] = trade
            return trade

    service = SchedulerService()
    register_fina_handlers(
        service,
        pricing_callable=lambda request: {"PV": 1.0},
        trade_repository=Trades(),
        olap_callable=olap_callable,
        risk_callable=scheduler_adapter.run_risk_task,
    )

    definition = render_parameters(
        yaml.safe_load(Path(args.process).read_text()),
        {
            "eval_datetime": "2027-05-18T08:00:00Z",
            "count": args.count,
            "seed": args.seed,
            "paths": args.paths,
        },
    )
    process = service.create_process(definition)
    snapshot = service.snapshot(process.id)[0]
    print(
        json.dumps(
            {
                "process": snapshot["name"],
                "state": snapshot["state"],
                "threads": [
                    {"name": t["name"], "state": t["state"], "priority": t["priority"]} for t in snapshot["threads"]
                ],
            },
            indent=2,
        )
    )

    risk = service.result(process.id, "risk")
    print("risk store:", json.dumps(risk.get("store", {}), indent=2, default=str))

    rows = query_ssrm({"dataset": "risk_wide", "startRow": 0, "endRow": 5})
    print(json.dumps({"olap_rows": len(rows["rows"]), "first": rows["rows"][0] if rows["rows"] else None}, default=str))


if __name__ == "__main__":
    main()
