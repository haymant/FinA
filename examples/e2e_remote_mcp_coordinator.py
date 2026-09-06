"""Run the fina-e2e-coordinator flow against deployed pricer and trade MCP servers.

The scheduler is the only workflow entrance. Remote MCP calls are injected as
lambda capabilities into the registered handlers; no pricer or trade operation
is invoked by the coordinator outside the submitted FinaProcess.
"""
from __future__ import annotations

import json
import sys
import time
import types
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

# The coordinator only needs the pure-Python process scheduler. Avoid importing
# fina_core.__init__ so this executable remains runnable before maturin builds
# the optional Rust extension.
if "fina_core" not in sys.modules:
    package = types.ModuleType("fina_core")
    package.__path__ = [str(ROOT / "python" / "fina_core")]
    sys.modules["fina_core"] = package

from fina_core.integrations import register_fina_handlers
from fina_core.process_scheduler import Event, SchedulerService

PRICER_URL = "https://fina-pricer.vercel.app/mcp"
TRADE_URL = "https://fina-trade-ten.vercel.app/mcp"

RFQ = {
    "InstrumentKey": {
        "isin": "XSREMOTE00001", "name": "FCN_REMOTE_E2E", "strategy_id": "STRATEGY_FCN_REMOTE",
        "product_type": "FCN", "family": "EQD", "group": "OPT", "leg_id": 1,
        "leg_name": "PUT", "notional": 100000.0, "payment_currency": "USD", "status": "LIVE",
    },
    "UnwindMapRaw": {"underlyings": [{
        "name": "AAPL US", "currency": "USD", "spot": 190.0, "strikePrice": 171.0,
        "barrierPrice": 152.0, "fx_pair": "USDUSD", "calendar": "NYSE", "time": "1600", "time_zone": "America/New_York",
    }]},
    "RiskFactorKeys": [
        {"type": "Spot", "underlying": "AAPL US", "temporal_role": "ValuationDate", "date": "2026-09-06"},
        {"type": "Volatility", "underlying": "AAPL US", "expiry": "2027-09-06", "strike": 171.0},
        {"type": "InterestRate", "underlying": "USD", "tenor": "1Y", "date": "2026-09-06"},
    ],
    "MarketDataSnapshot": {
        "spot_data": [{"rfk": {"underlying": "AAPL US"}, "value": 190.0}],
        "vol_data": [{"rfk": {"underlying": "AAPL US", "expiry": "2027-09-06", "strike": 171.0}, "value": 0.25}],
        "ir_data": [{"rfk": {"currency": "USD", "tenor": "1Y"}, "value": 0.04}],
        "fx_data": [],
    },
    "UpdatedLifecycle": {"instrument_state": "LIVE", "applied_fixings": [], "adjusted_underlyings": [{"name": "AAPL US", "adjustment_factor": 1.0}]},
    "parameters": {
        "eval_datetime": "2026-09-06", "expiry": "2027-09-06", "option_type": "put", "payoff_type": "fcn",
        "accrual": {"coupon_rate": 0.12, "memory": True, "observation_frequency": "monthly", "observations": 12, "pay_if_ki": True},
        "risk_free_rate": 0.04, "dividend_yield": 0.0, "volatility": 0.25,
        "paths": 1200, "steps": 24, "seed": 91, "bump_size": 0.0001, "bump_mode": "relative", "currency_conversion": 1.0,
    },
}


class RemoteMCP:
    def __init__(self, url: str):
        self.url = url
        self.session = ""
        self._initialize()

    def _post(self, payload: dict[str, Any], with_session: bool = True) -> dict[str, Any]:
        request = urllib.request.Request(self.url, data=json.dumps(payload).encode(), method="POST")
        request.add_header("content-type", "application/json")
        request.add_header("accept", "application/json, text/event-stream")
        if with_session and self.session:
            request.add_header("mcp-session-id", self.session)
        with urllib.request.urlopen(request, timeout=180) as response:
            if "mcp-session-id" in response.headers:
                self.session = response.headers["mcp-session-id"]
            body = response.read().decode()
        data_lines = [line[6:] for line in body.splitlines() if line.startswith("data: ")]
        if not data_lines:
            if not body.strip():
                return {"result": {}}
            raise RuntimeError(f"MCP response contained no SSE data: {body[:500]}")
        return json.loads(data_lines[-1])

    def _initialize(self) -> None:
        result = self._post({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "fina-e2e-coordinator", "version": "1"}}}, with_session=False)
        if "error" in result:
            raise RuntimeError(result)
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def call(self, name: str, arguments: dict[str, Any]) -> Any:
        result = self._post({"jsonrpc": "2.0", "id": int(time.time_ns() % 2_000_000_000), "method": "tools/call", "params": {"name": name, "arguments": arguments}})
        if "error" in result:
            raise RuntimeError(result)
        payload = result["result"]
        if payload.get("isError"):
            raise RuntimeError(payload)
        if "structuredContent" in payload:
            return payload["structuredContent"]
        text = next(item["text"] for item in payload.get("content", []) if item.get("type") == "text")
        return json.loads(text)


class RemoteTradeRepository:
    def __init__(self, client: RemoteMCP, scheduler: SchedulerService | None = None):
        self.client = client
        self.scheduler = scheduler
        self.trades: dict[str, dict[str, Any]] = {}
        self.publish = None
        self.rfq_id = "RFQ-REMOTE-E2E-" + str(int(time.time()))
        self.quote_id = "Q-REMOTE-E2E-" + str(int(time.time()))

    def register(self, trade: dict[str, Any]) -> dict[str, Any]:
        quote = trade["quote"]
        rfq = {"rfq_id": self.rfq_id, "correlation_id": "remote-coordinator-e2e", "client_id": "e2e-client", "instrument_id": trade["instrument_id"], "product_type": trade["product_type"], "request": RFQ}
        self.client.call("rfq_create", {"rfq": rfq})
        persisted_quote = self.client.call("quote_persist", {"quote": {"quote_id": self.quote_id, "rfq_id": self.rfq_id, "pricing_request": RFQ, "quote": quote}})
        row = self.client.call("trade_accept", {"trade": {**trade, "quote_id": self.quote_id, "terms": {"underlying": "AAPL US", "coupon": 0.12}}})
        self.trades[row["trade_id"]] = row
        return row

    def amend(self, trade_id: str, changes: dict[str, Any], reason: str) -> dict[str, Any]:
        row = self.client.call("trade_amend", {"trade_id": trade_id, "changes": {"terms": changes}, "reason": reason})
        self.trades[trade_id] = row
        if self.publish:
            self.publish({"topic": "trade.lifecycle.amended", "event_id": f"remote:{trade_id}:{int(time.time_ns())}", "trade_id": trade_id, "payload": {"trade_id": trade_id, "changes": changes, "reason": reason, "after_state": row}})
        return row


def run() -> dict[str, Any]:
    pricer = RemoteMCP(PRICER_URL)
    trade_mcp = RemoteMCP(TRADE_URL)
    scheduler = SchedulerService()
    trades = RemoteTradeRepository(trade_mcp, scheduler)
    trade_id = "T-REMOTE-E2E-" + str(int(time.time()))
    pricing_calls: list[dict[str, Any]] = []

    def pricing(request: dict[str, Any]) -> dict[str, Any]:
        pricing_calls.append(request)
        return pricer.call("pricing_and_sensitivity", {"request": {key: value for key, value in request.items() if key != "lifecycle_event"}})

    def olap(payload: dict[str, Any]) -> dict[str, Any]:
        result = payload["latest_reprice"]["quote"]
        cells = result.get("RiskCube", {}).get("cells", [])
        deltas = [cell.get("sensitivities", {}).get("delta", 0.0) for cell in cells]
        return {"group_by": "product_type", "rows": [{"product_type": "FCN", "trade_count": len(payload["trades"]), "avg_delta": sum(deltas) / len(deltas) if deltas else 0.0}], "source": "reprice.RiskCube.cells"}

    register_fina_handlers(scheduler, pricing_callable=pricing, trade_repository=trades, olap_callable=olap)
    process = scheduler.create_process({
        "api_version": "fina/v1", "kind": "FinaProcess", "metadata": {"name": "remote-fcn-rfq-to-risk"},
        "parameters": {"pricing_request": RFQ, "trade_id": trade_id, "correlation_id": "remote-coordinator-e2e"},
        "threads": [
            {"name": "quote", "handler": "fina-pricer.pricing_and_sensitivity", "parameters": {"trade_id": trade_id}},
            {"name": "register_trade", "handler": "fina-trade.register", "depends_on": ["quote"], "parameters": {"trade": {"trade_id": trade_id, "instrument_id": "FCN_REMOTE_E2E", "product_type": "FCN", "notional": 100000.0, "currency": "USD", "status": "LIVE"}}},
            {"name": "amend", "handler": "fina-trade.amend", "depends_on": ["register_trade"], "parameters": {"trade_id": trade_id, "changes": {"observation_date": "2026-10-06"}, "reason": "coordinator-lifecycle-e2e"}},
            {"name": "reprice", "handler": "fina-pricer.pricing_and_sensitivity", "triggered_by": "trade.lifecycle.amended", "parameters": {"trade_id": trade_id}},
            {"name": "olap", "handler": "fina-olap.group_sensitivities", "depends_on": ["reprice"]},
        ],
        "subscriptions": [{"topic": "trade.lifecycle.amended", "handler": "fina-pricer.pricing_and_sensitivity", "start_thread": "reprice"}],
    })
    snapshot = scheduler.snapshot(process.id)[0]
    lifecycle = trade_mcp.call("trade_lifecycle", {"trade_id": trade_id})
    assert process.state == "FINISHED", snapshot
    assert len(pricing_calls) == 2, len(pricing_calls)
    assert trades.trades[trade_id]["status"] == "AMENDED"
    assert any(item["event_type"] == "amended" for item in lifecycle["result"])
    assert scheduler.result(process.id, "olap")["rows"][0]["trade_count"] == 1
    return {"process_id": process.id, "process_state": process.state, "pricing_calls": len(pricing_calls), "rfq_id": trades.rfq_id, "quote_id": trades.quote_id, "trade": trades.trades[trade_id], "lifecycle_events": [item["event_type"] for item in lifecycle["result"]], "olap": scheduler.result(process.id, "olap"), "snapshot": snapshot}


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=str))
