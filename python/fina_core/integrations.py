"""Concrete handler wiring used by the single-entry Fina coordinator.

Adapters are dependency-injected: the scheduler does not import a particular
MCP client or trade database. Pass a callable that invokes the real pricer tool,
a TradeRepository-compatible object, and an OLAP callable.
"""
from __future__ import annotations

from typing import Any, Callable, Dict

from .process_scheduler import Event, SchedulerService, ThreadInstance


def register_fina_handlers(
    scheduler: SchedulerService,
    *,
    pricing_callable: Callable[[Dict[str, Any]], Dict[str, Any]],
    trade_repository: Any,
    olap_callable: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> None:
    """Register the canonical handler names used by FinaProcess YAML."""

    def price(thread: ThreadInstance, runtime: SchedulerService) -> Dict[str, Any]:
        request = dict(thread.parameters.get("pricing_request", {}))
        event = thread.parameters.get("event")
        if event:
            request["lifecycle_event"] = event
        quote = pricing_callable(request)
        return {"trade_id": thread.parameters.get("trade_id"), "quote": quote, "trigger": event}

    def register_trade(thread: ThreadInstance, runtime: SchedulerService) -> Dict[str, Any]:
        quote_result = runtime.result(thread.process_id, "quote")
        quote = quote_result.get("quote", quote_result)
        trade = dict(thread.parameters["trade"])
        trade["quote"] = quote
        return trade_repository.register(trade)

    def amend_trade(thread: ThreadInstance, runtime: SchedulerService) -> Dict[str, Any]:
        changes = dict(thread.parameters.get("changes", {}))
        return trade_repository.amend(thread.parameters["trade_id"], changes, thread.parameters.get("reason", "lifecycle event"))

    def grouped_olap(thread: ThreadInstance, runtime: SchedulerService) -> Dict[str, Any]:
        return olap_callable({"trades": list(trade_repository.trades.values()), "latest_reprice": runtime.result(thread.process_id, "reprice"), "parameters": thread.parameters})

    scheduler.register_handler("fina-pricer.pricing_and_sensitivity", price)
    scheduler.register_handler("fina-trade.register", register_trade)
    scheduler.register_handler("fina-trade.amend", amend_trade)
    scheduler.register_handler("fina-olap.group_sensitivities", grouped_olap)
    if hasattr(trade_repository, "publish"):
        trade_repository.publish = lambda event: scheduler.bus.publish(
            Event(event["topic"], event.get("payload", {}), event_id=event.get("event_id", ""), thread_id=event.get("trade_id"))
        )
