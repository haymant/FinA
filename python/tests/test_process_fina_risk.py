"""Scheduler wiring for the fina-risk compute handler."""

from fina_core.integrations import register_fina_handlers
from fina_core.process_scheduler import SchedulerService


def test_fina_risk_batch_handler_runs_via_risk_callable():
    service = SchedulerService()
    calls = []

    class Trades:
        trades: dict = {}

        def register(self, trade):
            self.trades[trade["trade_id"]] = trade
            return trade

    def price(request):
        return {"PV": 1.0}

    def olap(payload):
        return {"ok": True}

    def risk(request):
        calls.append(request)
        return {"status": "ok", "instruments": request.get("instruments", 0)}

    register_fina_handlers(service, pricing_callable=price, trade_repository=Trades(), olap_callable=olap, risk_callable=risk)
    definition = {
        "metadata": {"name": "risk-batch"},
        "parameters": {"risk_request": {"instruments": 7, "paths": 64, "seed": 1, "persist": False}},
        "threads": [{"name": "risk", "handler": "fina-risk.risk_batch", "priority": 4}],
    }
    process = service.create_process(definition)
    thread = process.threads[process.id + "/risk"]
    assert thread.state == "FINISHED"
    assert thread.result["instruments"] == 7
    assert thread.priority == 4
    assert calls and calls[0]["instruments"] == 7
