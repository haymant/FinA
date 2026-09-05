"""Single-entry FinA scheduler verification with real pricer and trade handlers.

Run after installing fina-core and fina-pricer environments with both repositories
on PYTHONPATH. The pricing callable below is the same implementation exposed by
fina-pricer's pricing_and_sensitivity MCP tool.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
sys.path.insert(0, str(ROOT / "modules" / "fina-trade"))
sys.path.insert(0, str(ROOT.parent / "fina-pricer" / "src"))

from fina_core.integrations import register_fina_handlers
from fina_core.process_scheduler import SchedulerService
from fina_trade import TradeRepository
from riskcube_mcp import PricingRequest, sensitivity


RFQ = {
    "InstrumentKey": {"isin": "XS9999999999", "name": "FCN_SAMPLE_001", "strategy_id": "STRATEGY_FCN_001", "product_type": "FCN", "family": "EQD", "group": "OPT", "leg_id": 1, "leg_name": "PUT", "notional": 770000.0, "payment_currency": "USD", "status": "LIVE"},
    "UnwindMapRaw": {"underlyings": [{"name": "UND_A HK", "currency": "HKD", "spot": 6.3612, "strikePrice": 5.5858, "barrierPrice": 6.3612, "fx_pair": "USDHKD", "calendar": "EXCH HKG", "time": "1600", "time_zone": "Hong Kong"}]},
    "RiskFactorKeys": [{"type": "Spot", "underlying": "UND_A HK", "temporal_role": "ValuationDate", "date": "2027-05-18"}, {"type": "Volatility", "underlying": "UND_A HK", "expiry": "2028-05-25", "strike": 5.5858}, {"type": "InterestRate", "underlying": "USD", "tenor": "5Y", "date": "2027-05-18"}, {"type": "FXSpot", "currency_pair": "USDHKD", "date": "2027-05-18"}],
    "MarketDataSnapshot": {"spot_data": [{"rfk": {"underlying": "UND_A HK"}, "value": 6.3612}], "vol_data": [{"rfk": {"underlying": "UND_A HK", "expiry": "2028-05-25", "strike": 5.5858}, "value": 0.25}], "ir_data": [{"rfk": {"currency": "USD", "tenor": "5Y"}, "value": 0.03}], "fx_data": [{"rfk": {"currency_pair": "USDHKD"}, "value": 7.8}]},
    "UpdatedLifecycle": {"instrument_state": "LIVE", "applied_fixings": ["2027-05-18"], "adjusted_underlyings": [{"name": "UND_A HK", "adjustment_factor": 1.0}]},
    "parameters": {"eval_datetime": "2027-05-18", "expiry": "2028-05-25", "option_type": "put", "payoff_type": "fcn", "accrual": {"coupon_rate": 0.12, "memory": True, "observation_frequency": "monthly", "observations": 12, "pay_if_ki": True}, "risk_free_rate": 0.03, "dividend_yield": 0.0, "volatility": 0.25, "paths": 4000, "steps": 48, "seed": 42, "bump_size": 0.0001, "bump_mode": "relative", "currency_conversion": 1.0},
}


def verify() -> dict:
    scheduler = SchedulerService()
    trades = TradeRepository()
    pricing_calls = []

    def pricing_tool(request):
        pricing_calls.append(request)
        clean = {key: value for key, value in request.items() if key != "lifecycle_event"}
        result = sensitivity(PricingRequest.model_validate(clean))
        return {"PV": result["PV"], "PV_currency": result["PV_currency"], "price_pct_of_notional": result["price_pct_of_notional"], "RiskCube": result["RiskCube"]}

    def grouped_olap(payload):
        cells = payload["latest_reprice"]["quote"]["RiskCube"]["cells"]
        deltas = [cell["sensitivities"].get("delta", 0.0) for cell in cells]
        return {"group_by": "product_type", "rows": [{"product_type": "FCN", "trade_count": len(payload["trades"]), "avg_delta": sum(deltas) / len(deltas)}]}

    register_fina_handlers(scheduler, pricing_callable=pricing_tool, trade_repository=trades, olap_callable=grouped_olap)
    definition = {"api_version": "fina/v1", "kind": "FinaProcess", "metadata": {"name": "real-fcn-rfq-to-risk"}, "parameters": {"pricing_request": RFQ, "trade_id": "FCN-T1"}, "threads": [
        {"name": "quote", "handler": "fina-pricer.pricing_and_sensitivity", "parameters": {"trade_id": "FCN-T1"}},
        {"name": "register_trade", "handler": "fina-trade.register", "depends_on": ["quote"], "parameters": {"trade": {"trade_id": "FCN-T1", "instrument_id": "FCN_SAMPLE_001", "product_type": "FCN", "notional": 770000.0, "currency": "USD", "status": "LIVE"}}},
        {"name": "amend", "handler": "fina-trade.amend", "depends_on": ["register_trade"], "parameters": {"trade_id": "FCN-T1", "changes": {"observation_date": "2027-06-18"}, "reason": "fixing observation"}},
        {"name": "reprice", "handler": "fina-pricer.pricing_and_sensitivity", "triggered_by": "trade.lifecycle.amended", "parameters": {"trade_id": "FCN-T1"}},
        {"name": "olap", "handler": "fina-olap.group_sensitivities", "depends_on": ["reprice"]}], "subscriptions": [{"topic": "trade.lifecycle.amended", "handler": "fina-pricer.pricing_and_sensitivity", "start_thread": "reprice"}]}
    process = scheduler.create_process(definition)
    assert process.state == "FINISHED"
    assert len(pricing_calls) == 2
    assert trades.get("FCN-T1")["status"] == "AMENDED"
    assert process.threads[process.id + "/reprice"].state == "FINISHED"
    assert process.threads[process.id + "/olap"].result["rows"][0]["trade_count"] == 1
    return {"process_id": process.id, "pricing_calls": len(pricing_calls), "first_quote": scheduler.result(process.id, "quote"), "reprice": scheduler.result(process.id, "reprice"), "olap": scheduler.result(process.id, "olap"), "trade_events": [event["topic"] for event in trades.events]}


if __name__ == "__main__":
    print(verify())
