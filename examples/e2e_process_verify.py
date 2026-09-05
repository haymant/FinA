"""Executable FinA v1 coordinator verification.

The production handlers are injected here to keep this example deterministic;
replace them with fina-pricer and fina-olap MCP adapters in deployment.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
sys.path.insert(0, str(ROOT / "modules" / "fina-trade"))

from fina_core.process_scheduler import SchedulerService
from fina_trade import TradeRepository


def verify() -> dict:
    service = SchedulerService()
    prices = []
    events = []
    trades = TradeRepository(events.append)

    def quote(thread, _runtime):
        result = {"trade_id": "FCN-T1", "pv": 98.2, "delta": -0.31, "vega": 0.12}
        prices.append(result)
        return result

    def register(_thread, _runtime):
        return trades.register({"trade_id": "FCN-T1", "instrument_id": "FCN-1", "product_type": "FCN", "notional": 100000, "currency": "USD", "quote": prices[-1], "status": "LIVE"})

    def amend(_thread, _runtime):
        return trades.amend("FCN-T1", {"quote": {"pv": 97.7, "delta": -0.36}}, "observation")

    def reprice(_thread, _runtime):
        return {"trade_id": "FCN-T1", "pv": 97.7, "delta": -0.36, "vega": 0.13, "trigger": "amend"}

    def olap(_thread, _runtime):
        return {"group_by": "product_type", "rows": [{"product_type": "FCN", "trade_count": 1, "avg_delta": -0.36, "avg_vega": 0.13}]}

    for name, handler in [("quote", quote), ("register", register), ("amend", amend), ("reprice", reprice), ("olap", olap)]:
        service.register_handler(name, handler)
    process = service.create_process({"api_version": "fina/v1", "kind": "FinaProcess", "metadata": {"name": "fcn-rfq-to-risk"}, "parameters": {"eval_datetime": "${eval_datetime}"}, "threads": [
        {"name": "quote", "handler": "quote"}, {"name": "register_trade", "handler": "register", "depends_on": ["quote"]},
        {"name": "amend", "handler": "amend", "depends_on": ["register_trade"]}, {"name": "reprice", "handler": "reprice", "depends_on": ["amend"]},
        {"name": "olap", "handler": "olap", "depends_on": ["reprice"]}]}, {"eval_datetime": "2027-05-18T08:00:00Z"})
    assert process.state == "FINISHED"
    assert all(thread.state == "FINISHED" for thread in process.threads.values())
    assert trades.get("FCN-T1")["status"] == "AMENDED"
    assert [event["topic"] for event in trades.events] == ["trade.lifecycle.registered", "trade.lifecycle.amended"]
    assert process.threads[process.id + "/olap"].result["rows"][0]["avg_delta"] == -0.36
    return {"process_id": process.id, "trade_events": [event["topic"] for event in trades.events], "olap": process.threads[process.id + "/olap"].result}


if __name__ == "__main__":
    print("E2E VERIFIED", verify())
